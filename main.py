# -*- coding: utf-8 -*-
import codecs  # использую для чтени русских символов из файла
import vk  # использую для работы с api Вконтакте
from datetime import datetime, timezone, date  # использую для работы с датой и временем
from dostoevsky.models import FastTextSocialNetworkModel  # модель, натренерованная на русском языке
from dostoevsky.tokenization import RegexTokenizer  # используется для токенизации текста Достоевским
import pandas as pd  # использую для создания датафреймов

token = 'ff7aa2adff7aa2adff7aa2ad1bff0f2f69fff7aff7aa2ada0a76b490eca956fc1beb15b'  # ключ доступа
session = vk.Session(access_token=token)  # открываю сессию для работе с модулем vk
vk_api = vk.API(session)  # получаю доступ к методам Api Вконтакте
vk_api_v = 5.126  # последняя версия Api Вконтакте

# Список адресов групп в Вконтакте
group_list = ['msu_official', 'mgimo', 'miptru', 'hse_university', 'mephi_official', 'tpunews', 'reu',
              'sziu_ranepa', 'nsu24', 'finuniversity', 'bmstu1830', 'nust_misis', 'pgpuspb', 'rudn_university',
              'kazan_federal_university', 'spbgmupavlova', 'mslu_studlife', 'vavt_official', 'tomskuniversity']


# Функция определяет встречается ли в тексте хотя бы одно из ключевых слов
def contains_keywords(text, keyword_list):
    for keyword in keyword_list:  # итерирую по списку ключевых слов
        if text.find(keyword):  # если найдено ключевое слово
            return True
    return False


# Функция возвращает список ккомментариев со всеми подкомментариями для поста
def get_comments(group_id, post, timestamp, current_comment_item_id=0):
    comments = []  # список комментариев
    count_limit = 100  # количество получаемых комментариев за один запрос
    response = vk_api.wall.getComments(owner_id=-group_id, post_id=post, comment_id=current_comment_item_id,
                                       need_likes=0, count=1, v=vk_api_v)  # получаю первый комментарий
    if not len(response['items']):  # если у поста нет ни одного комментария
        return []  # возвращаю пустой список комментариев

    comment_item = response['items'][0]  # получаю первый комментарий
    if not ('deleted' in comment_item):  # если он не удалён
        comment_item_id = response['items'][0]['id']  # получаю id комментария
        comment_item_text = response['items'][0]['text']  # получаю текст комментария
        comment_item_date = response['items'][0]['date']  # получаю время и дату комментария
        if comment_item_text:  # если у комментария есть текст
            comments.append({'text': comment_item_text, 'date': comment_item_date})  # добавляю комментарий в список
        comments += get_comments(group_id, post, timestamp, comment_item_id)  # рекурсивно получаю список
        # подкомментариев

    item_count = response['count']  # получаю общее количество комментариев
    for offset in range(0, item_count // count_limit + 1):  # высчитываю количество требуемых запросов и начинаю цикл
        response = vk_api.wall.getComments(owner_id=-group_id, post_id=post, comment_id=current_comment_item_id,
                                           need_likes=0, count=count_limit, offset=1 + count_limit * offset,
                                           v=vk_api_v)  # получаю порцию комментариев
        for comment_item in response['items']:  # произвожу итерации по всем полученным комментариям
            if not ('deleted' in comment_item):  # если он не удалён
                comment_item_id = comment_item['id']  # получаю id комментария
                comment_item_text = comment_item['text']  # получаю текст комментария
                comment_item_date = comment_item['date']  # получаю время и дату комментария
                if comment_item_text:  # если у комментария есть текст
                    comments.append({'text': comment_item_text, 'date': comment_item_date})  # добавляю комментарий в
                    # список
                comments += get_comments(group_id, post, timestamp, comment_item_id)  # рекурсивно получаю список
        # подкомментариев
    return comments  # возвращаю список комментариев


# Функция получает списко постов группы, удовлетворяющих условиям, и получает списко их комментариев
def get_all_comments(group_domain, date_bound, keyword_list):
    timestamp = date_bound.replace(tzinfo=timezone.utc).timestamp()  # перевожу дату в таймштамп без временной зоны
    count_limit = 100  # количество получаемых постов за один запрос
    post_count = 0  # количество постов, удовлетворяющих условиям
    comments = []  # список комментариев

    group_info = vk_api.groups.getById(group_ids=group_domain, v=vk_api_v)  # получаю информацию о группе
    group_id = group_info[0]['id']  # получаю id группы

    response = vk_api.wall.get(domain=group_domain, count=1, v=vk_api_v)  # получаю первый пост
    if not len(response['items']):  # если на странице нет ни одного поста
        return []  # возвращаю пустой список комментариев

    post = response['items'][0]  # получаю первый пост
    if contains_keywords(post['text'], keyword_list) and post['comments']['count']:  # если у поста есть комментарии
        comments += get_comments(group_id, post['id'], timestamp)  # получаю их
        post_count += 1  # увеличиваю счётчик постов

    item_count = response['count']  # сохраняю общее количество комментариев
    for offset in range(0, item_count // count_limit + 1):  # высчитываю количество требуемых запросов и начинаю цикл
        response = vk_api.wall.get(domain=group_domain, count=count_limit,
                                   offset=1 + count_limit * offset, v=vk_api_v)  # получаю порцию постов
        for post in response['items']:  # произвожу итерации по всем полученным постам
            if post['date'] < timestamp:  # если встретила пост раньше указанного времени
                break  # остановаливаю итерирование
            if contains_keywords(post['text'], keyword_list) and post['comments']['count']:  # если пост содержит
                # ключевые слова и есть комментарии
                comments += get_comments(group_id, post['id'], timestamp)  # получаю комментарии
                post_count += 1  # увеличиваю счётчик постов
    print(f'С {group_domain} получено постов: {post_count}, комментариев: {len(comments)}')  # вывожу количество постов
    # и комментариев по группе
    return comments  # возвращаю список найденных комментариев


# Точка входа в модуль
if __name__ == '__main__':  # начало выполнения программы
    with codecs.open('keywords.txt', encoding='cp1251') as file:  # открываю файл с ключевыми словами
        keywords = file.readlines()  # считываю все строчки из файла
    keywords = [keyword.strip() for keyword in keywords]  # удаляю \n

    for group_uri in group_list:  # соберу данные по каждой группе
        print('Группа: ', group_uri)  # вывожу адрес текущей группы
        all_comments = get_all_comments(group_uri, datetime(2020, 2, 1), keywords)  # вызываю метод получения всех
        # комментариев, указываю дату верхней границы времени, передаю список ключевых слов

        # инициализирую модуль Достоевского
        tokenizer = RegexTokenizer()
        model = FastTextSocialNetworkModel(tokenizer=tokenizer)

        comments_text = [comment['text'] for comment in all_comments]  # получаю список, содержащий только текст
        # комментариев
        results = model.predict(comments_text)  # произвожу анализ Достоевским

        comments_df = pd.DataFrame(columns=['Год', 'Месяц', 'Комментарий', 'Позитивность', 'Негативность'])  # создаю
        # датафрейм для агрегации данных по комментариям
        levels = {}  # словарь для промежуточных вычислений
        for comment, sentiment in zip(all_comments, results):  # склеиваю списки с комментариями и результатом
            # работы Довстоевского
            comment_date = datetime.fromtimestamp(comment['date'])  # получаю дату из таймштампа
            year = comment_date.year  # получаю год комментария
            month = comment_date.month  # получаю месяц комментария
            comment_date = date(year, month, 1)  # создаю дату начала месяца
            if not (comment_date in levels):  # если ещё не встречался ни один комментарий из данного месяца
                levels[comment_date] = {'positive': [], 'negative': []}  # инициализирую словарь для текущего месяца
            levels[comment_date]['positive'].append(sentiment['positive'])  # добавляю позитивный коэффициент
            levels[comment_date]['negative'].append(sentiment['negative'])  # добавляю негативный коэффициент
            # формирую датафрейм для текущего комментария и добавляю его в агрегирующий
            comments_df = comments_df.append(
                pd.DataFrame([[year, month, comment['text'], sentiment['negative'], sentiment['positive']]],
                             columns=['Год', 'Месяц', 'Комментарий', 'Позитивность', 'Негативность']),
                ignore_index=True)  # игнорирую индекс 0 из нового датафрейма

        coefficients_df = pd.DataFrame(columns=['Год', 'Месяц', 'Позитивность', 'Негативность'])  # создаю
        # датафрейм для агрегации данных по коэффициентам
        tonality_coefficients = {}  # создаю словарь для хранения коэффициентов тональности по месяцам
        for comment_date, sentiments in levels.items():
            negative_level = sum(sentiments['negative']) / len(sentiments['negative'])  # считаю среднее по негативу
            positive_level = sum(sentiments['positive']) / len(sentiments['positive'])  # считаю среднее по позитиву
            tonality_coefficients[comment_date] = pow((negative_level / positive_level), 2)  # высчитываю коэффициент
            # тональности
            # формирую датафрейм для текущего комментария и добавляю его в агрегирующий
            coefficients_df = coefficients_df.append(
                pd.DataFrame([[comment_date.year, comment_date.month, negative_level, positive_level]],
                             columns=['Год', 'Месяц', 'Позитивность', 'Негативность']), ignore_index=True)  # игнорирую
            # индекс 0 из нового датафрейма

        keys = list(tonality_coefficients.keys())  # получаю список дат
        values = list(tonality_coefficients.values())  # получаю список коэффициентов
        keys, values = zip(*sorted(zip(keys, values)))  # сортирую по дате

        # вывожу датафреймы в консоль
        print(comments_df)
        print(coefficients_df)

        # Сохраняем датафрейм в Excel
        writer = pd.ExcelWriter(f'dataframes/{group_uri}.xlsx', engine='xlsxwriter')  # создаю и открываю файл на запись
        comments_df.to_excel(writer, sheet_name='Комментарии')  # сохраняю датафрейм с комментариями
        coefficients_df.to_excel(writer, sheet_name='Коэффициенты')  # сохраняю датафрейм с коэффициентами
        writer.save()  # сохраняю файл
