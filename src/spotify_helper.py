import os
import base64
import secrets
from pathlib import Path

import requests
from requests import post
from typing import Dict
from urllib.parse import quote_plus


from dotenv import set_key


class SpotifyAuthentication:

    _spotify_url = "https://accounts.spotify.com"
    _auth_endpoint = '/authorize'
    _access_token_endpoint = '/api/token'

    def __init__(self, client_id: str, scopes: str = None,
                 redirect_uri: str = None, client_secret: str = None,
                 state_code: str = None, credentials: Dict = None):
        self._client_id = client_id
        self._scopes = scopes
        self._redirect_uri = redirect_uri
        self._client_secret = client_secret
        self._state_code = state_code
        self.credentials = credentials
        encoded_vals = f"{self._client_id}:{self._client_secret}".encode("utf-8")
        self.default_header = {
            "Authorization": "Basic " + str(base64.b64encode(encoded_vals), 'utf-8'),
            "Content-Type": "application/x-www-form-urlencoded"}

    @property
    def redirect_uri(self):
        return self._redirect_uri

    @redirect_uri.setter
    def redirect_uri(self, value):
        self._redirect_uri = value

    @staticmethod
    def generate_csrf_state():
        """Used to generate a state code we check to ensure a request comes from our application"""
        return secrets.token_hex(16)

    @classmethod
    def from_client_config(cls, client_id: str, scopes: str = None, **kwargs):
        """Creates a :class:`SpotifyAuthentication` instance from env variables
                        Args:
                            client_id (str): The app's registered client id
                            scopes (str): A list of string scopes which will be joined by a comma','
                            kwargs: Any additional parameters passed to
                                :class:`SpotifyAuthentication`
                        Returns:
                            SpotifyAuthentication: The constructed :class:`SpotifyAuthentication` instance.
                        """

        redirect_uri = kwargs.pop("redirect_uri", None)
        client_secret = kwargs.pop("client_secret", None)
        state = kwargs.pop("state", None)

        return cls(client_id=client_id, scopes=scopes, redirect_uri=redirect_uri,
                   client_secret=client_secret, state_code=state)

    def authorization_url(self):
        """This builds the url we make a request to that brings up the login dialog"""
        self._state_code = self.generate_csrf_state()

        url = f"{self._spotify_url}{self._auth_endpoint}"
        url += f"?client_id={self._client_id}"
        url += f"&response_type=code"
        url += f"&redirect_uri={quote_plus(self._redirect_uri)}"
        url += f"&state={self._state_code}"
        url += f"&scope={self._scopes}"
        url += f"&show_dialog=True"

        return url, self._state_code

    def fetch_user_access_token(self, query_params: Dict):
        error = query_params.get("error", None)

        if error:
            return {'status': False, 'message': error}

        if self._state_code != query_params.get('state'):
            return {'status': False, 'message': 'inconsistent state code'}

        url = f"{self._spotify_url}{self._access_token_endpoint}"
        params = {"code": query_params.get('code'), "grant_type": "authorization_code",
                  "redirect_uri": self._redirect_uri}
        response = post(url=url, params=params, headers=self.default_header)

        response_data = response.json()
        self.credentials = response_data

        self.write_to_env_file(SPOTIFY_ACCESS_TOKEN=self.credentials['access_token'],
                                     SPOTIFY_REFRESH_TOKEN=self.credentials['refresh_token'])
        return {'status': True}

    @staticmethod
    def write_to_env_file(**kwargs) -> None:
        env_path = os.getenv('ENV_FILE_PATH')
        env_file_path = Path(env_path)

        # Create the file if it does not exist.
        env_file_path.touch(mode=0o600)
        for keys, values in kwargs.items():
            set_key(dotenv_path=env_file_path, key_to_set=keys, value_to_set=values)

    @classmethod
    def request_refreshed_access_token(cls, refresh_token: str, client_secret: str, client_id: str):
        encoded_vals = f"{client_id}:{client_secret}".encode("utf-8")
        header = {
            "Authorization": "Basic " + str(base64.b64encode(encoded_vals), 'utf-8'),
            "Content-Type": "application/x-www-form-urlencoded"}
        params = {'grant_type': 'refresh_token', 'refresh_token': refresh_token}

        url = f"{cls._spotify_url}{cls._access_token_endpoint}"
        response = post(url=url, params=params, headers=header)
        response_data = response.json()
        cls.write_to_env_file(SPOTIFY_ACCESS_TOKEN=response_data['access_token'])
        return response_data['access_token']


class SpotifyData(SpotifyAuthentication):
    _api_url = 'https://api.spotify.com/v1'
    _recently_played_endpoint = '/me/player/recently-played'

    def __init__(self, access_token: str, refresh_token: str,
                 client_secret: str, client_id: str):
        super().__init__(client_id=client_id, client_secret=client_secret)
        self.refresh_token =refresh_token
        self.default_header = {"Content-Type": "application/json",
                               "Authorization": f"Bearer {access_token}"}

    @staticmethod
    def safeget(input_dict, *keys_to_find):
        if not input_dict:
            return None
        for key in keys_to_find:
            try:
                input_dict = input_dict[key]
            except KeyError:
                return None
        return input_dict

    def request_recently_played(self, query_params):
        spotify_data = []

        url = f"{self._api_url}{self._recently_played_endpoint}"
        count = 0
        while True:
            response = requests.get(url=url, params=query_params, headers=self.default_header)
            response_data = response.json()
            if self.safeget(response_data, 'error', 'message') == 'The access token expired':
                access_token = self.request_refreshed_access_token(
                    refresh_token=self.refresh_token, client_secret=self._client_secret,client_id=self._client_id)
                self.default_header = {"Content-Type": "application/json",
                               "Authorization": f"Bearer {access_token}"}

                continue

            response_items = response_data['items']
            for index in range(len(response_items)):
                track_details = self.extract_track_details(response_items[index])
                spotify_data.append(track_details)

            next_page = self.safeget(response_data, 'next')
            if next_page is None:
                break

            url = next_page
            query_params = {}
            count += 1
            print(count)
        return spotify_data

    def extract_track_details(self, response_items):
        #track
        track = self.safeget(response_items, 'track')
        track_name = self.safeget(track, 'name')
        track_available_market = len(self.safeget(track, 'available_markets'))
        track_duration = self.safeget(track, 'duration_ms')
        track_popularity = self.safeget(track, 'popularity')

        #track/artist
        artists = self.safeget(track, 'artists')
        artist_name = self.safeget(artists[0], 'name')
        artist_followers = self.safeget(artists[0], 'followers', 'total')

        featured_artists = [self.safeget(artists[index], 'name') for index in range(1, len(artists))]
        if not featured_artists:
            featured_artists = None
        else:
            featured_artists = ', '.join(featured_artists)

        #track/album
        album = self.safeget(track, 'album')
        if self.safeget(album, 'album_type') == 'album':
            in_album = True
        else:
            in_album = False
        album_countries_available = len(self.safeget(album, 'available_markets'))
        album_name = self.safeget(album, 'name')

        # played at
        played_at = self.safeget(response_items, 'played_at')
        time_stamp = played_at[:10]

        # context
        played_from = self.safeget(response_items, 'context', 'type')
        track_details = self.safeget(response_items, 'context', 'href')
        return {"track_name": track_name,
                'track_popularity': track_popularity,
                "track_countries_available": track_available_market,
                'track_duration_ms': track_duration,
                "artist_name": artist_name,
                "artist_followers": artist_followers,
                'featured_artist': featured_artists,
                'in_album': in_album,
                'album_countries_available': album_countries_available,
                'album_name': album_name,
                'played_at': played_at,
                'time_stamp': time_stamp,
                'played_from': played_from,
                'track_details': track_details}