import re


def find_programing_language(body):
    # Make a list of regex
    language_lst = ["Java", "Python", "C++", "C#", "Go", "Ruby", "Javascript", "PHP", "HTML", "CSS", "SQL"]

    # Find programing language
    programing_languages_lst = []
    for programing_language in language_lst:
        if programing_language in body:
            programing_languages_lst.append(programing_language)

    return programing_languages_lst


def get_url(body):
    re_url = r'href="([0-9a-zA-Z_.:\/]*)"'
    return re.findall(re_url, body)
