import html
import re



class TextEditor:
    def __init__(self, row_text):
        self.row_text = row_text
        

    def clean_html(self):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', self.row_text)
        clean_text = html.unescape(cleantext)
        return clean_text


    def dollar_price_extraction(self):
        match = re.search(r'\$(\d+)', self.row_text)
        if match:
            return match.group(1)