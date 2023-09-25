class Page:
    def __init__(self, page_num):
        self.page_num = page_num
        self.page_content = ""

    def set_page_content(self, page_content):
        self.page_content = page_content

    def print_page_content(self):
        print(self.page_content)

    def set_page_num(self, page_num):
        self.page_num = page_num


class Chapter(Page):
    def __init__(self, page_num, chapter_num=0, chapter_title=""):
        super().__init__(page_num)
        self.chapter_num = chapter_num
        self.chapter_title = chapter_title

    def set_chapter_num(self, chapter_num):
        self.chapter_num = chapter_num

