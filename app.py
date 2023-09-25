import os

from flask import Flask, request, redirect, session
from flask_restful import Resource, Api
from dotenv import load_dotenv

from src.spotify_helper import SpotifyAuthentication


load_dotenv()


SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_SCOPES = os.getenv("SPOTIFY_SCOPES")
REDIRECT_URI = 'spotify/callback'

app = Flask(__name__)
app.secret_key = "Abiola Adeoye"
api = Api(app)


class SpotifyAuth(Resource):

    @classmethod
    def get(cls):
        auth_flow = SpotifyAuthentication.from_client_config(client_id=SPOTIFY_CLIENT_ID, scopes=SPOTIFY_SCOPES,)
        auth_flow.redirect_uri = request.url_root + REDIRECT_URI
        authorization_url, state = auth_flow.authorization_url()

        redirect_response = redirect(authorization_url)
        session['state'] = state
        return redirect_response


class SpotifyCallback(Resource):

    @classmethod
    def get(cls):
        state = session['state']

        flow = SpotifyAuthentication.from_client_config(client_id=SPOTIFY_CLIENT_ID, scopes=SPOTIFY_SCOPES,
                                                        client_secret=SPOTIFY_CLIENT_SECRET, state=state)

        flow.redirect_uri = request.url_root + REDIRECT_URI

        authorization_params = request.args
        get_token = flow.fetch_user_access_token(authorization_params)
        if get_token['status'] is True:
            return {'message': "spotify tokens extracted and saved to env file"}
        return {"message": "There was an error requesting for the tokens"}, 500


api.add_resource(SpotifyAuth, '/auth/spotify')
api.add_resource(SpotifyCallback, '/spotify/callback')

if __name__ == '__main__':
    app.run(debug=True)