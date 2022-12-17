import csv
import pandas as pd
import time
import math
import json

time_flag = df = None

''' 
декоратор
считает количество вызовов 
'''
def decor_count(func):
    def wrapper(*args, **kwargs):
        wrapper.counter += 1
        return func(*args, **kwargs)
    wrapper.counter = 0
    return wrapper

''' кэширование запроса'''
def cash_function(cashed_arguements, amount_of_calling):
    try:
        with open('technical_data_analysis_using_algorithms/cash.json', 'r') as my_favourite_json:
            data = json.load(my_favourite_json)
    except:
        data = {}
    try: 
        ''' 
        ищем запрос в кэшэ запросов
        нашли? вернем номер кэша
        '''
        return data[cashed_arguements]
    except: 
        '''
        не нашли? 
        запишем запрос в кэш запросов
        '''
        data[cashed_arguements] = amount_of_calling
        with open('technical_data_analysis_using_algorithms/cash.json', 'w') as my_favourite_json:
            json.dump(data, my_favourite_json)
        return 0

@decor_count
def get_sorted():
    sort_dict = {'1': 'open', '2': 'close', '3': 'high', '4': 'low', '5': 'volume'}
    sort_columns = order = limit = filename = None
    '''взаимодействие с пользователем при запуске скрипта'''
    while 1:
        print('Сортировать по цене\nоткрытия (1)\nзакрытия (2)\nмаксимум [3]\nминимум (4)\nобъем (5)')
        sort_columns = input()
        try:
            sort_columns = sort_dict[sort_columns]
            break
        except: pass
        if sort_columns == '':
            sort_columns = 'high'
            break
    while 1:
        order = input('Порядок по убыванию [1] / возрастанию (2): ')
        if order == '2':
            order = 'asc'
            break
        elif order == '1' or order == '':
            order = 'desc'
            break       
    while 1:
        limit = input('Ограничение выборки [10]: ')
        try:
            limit = int(limit)
            if limit >= 1: break
        except: 
            if limit == "":
                limit = 10
                break
            continue
    while 1:
        filename = input('Название файла для сохранения результата [dump.csv]: ')
        if filename == "":
            filename = 'dump.csv'
        if len(filename) >= 5 and filename[-5:-1] != '.csv': break

    '''
    кладем в переменную инфу для кэша
     - номер функции, передаваемые параметры
    '''
    some_cash =','.join(['get_sorted', sort_columns, str(order), str(limit)])
    '''
    запрос в кэш запросов
    данная функция с данными аргументами уже вызывалась?
    '''
    some_cash = cash_function(some_cash, get_sorted.counter)
    if some_cash == 0:
        '''
        функция не вызывалась
        работаем
        '''
        get_sorted_table = select_sorted(sort_columns, order, limit)
        print('go to cash')
        print('cash = ', some_cash)
        time.sleep(2)
        '''записываем информацию в кэш'''
        get_sorted_table.to_pickle('technical_data_analysis_using_algorithms/get_sorted' + str(get_sorted.counter) + '.pkl')
    else:
        print('take from cash = ', some_cash)
        
        '''
        функция вызывалась
        достаем информацию из кэша
        '''
        time.sleep(2)
        get_sorted_table = pd.read_pickle('technical_data_analysis_using_algorithms/get_sorted' + str(some_cash) + '.pkl')
    '''отправляем результат в csv файл'''
    get_sorted_table.to_csv('technical_data_analysis_using_algorithms/' + filename)
    
def select_sorted(sort_columns, order, limit):
    global df, start_time, time_flag
    df = pd.read_csv('technical_data_analysis_using_algorithms/all_stocks_5yr.csv') ## reading csv
    time_flag = True ## just a flag
    start_time = start_time_2 = time.time() ## program started
    dj = quick_sort(df, sort_columns)
    '''
    если нужна отсортированная таблица по убыванию
    воспользуемся встроенным инструментом Padnas'''
    if order == 'desc':
        print('desk')
        dj = cheet_sort(df, sort_columns, False)
    return dj.iloc[0:limit]

''' 
алгоритм быстрой сортировки
сортируем объект Pandas DataFrame
'''
def quick_sort(qbject, column):
    '''делим таблицу на три части'''
    pd_1 = pd.DataFrame(columns = list(qbject.columns)) ## больше искомого элемента
    pd_2 = qbject.iloc[[0]] ## равно
    pd_3 = pd.DataFrame(columns = list(qbject.columns)) ## меньше

    if len(qbject) == 1: return qbject ## выход из рекурсии
    '''сама сортировка'''
    for i in range(len(qbject)):
        if qbject.iloc[i][column] < qbject.iloc[0][column]:
            pd_1 = pd.concat([pd_1, qbject.iloc[[i]]])
        elif qbject.iloc[i][column] > qbject.iloc[0][column]:
            pd_3 = pd.concat([pd_3, qbject.iloc[[i]]])
            if check(i, qbject): ## how long it is hoing to sort?
                return cheet_sort(qbject, column) ## call sorthing with pandas
        else:
            pd_2 = pd.concat([pd_2, qbject.iloc[[i]]])
    try:
        pd_1 = pd.concat([quick_sort(pd_1, column), pd_2]) ## рекурсия по пустому DataFrame выдает ошибку, обрабатываем
    except:
        pd_1 = pd.concat([pd_1, pd_2])
    try:
        pd_1 = pd.concat([pd_1, quick_sort(pd_3, column)]) ## та же самая проблема с пустым DataFrame
    except:
        pd_1 = pd.concat([pd_1, pd_3])
    return pd_1

'''сортировка wредствами Pandas'''
def cheet_sort(qbject, column, asc = True):
    qbject = qbject.sort_values(by = column, ascending = asc)
    return qbject

'''проверка времени выполнения алгоритма'''
def check(i, qbject):
    global time_flag, start_time
    if time_flag and i > 400:
        how_time = time.time() - start_time
        big_o = len(qbject) * math.log(len(qbject))
        how_long = (big_o / i) * how_time
        print(f'Алгоритм работает уже {how_time} секунд')
        print('Осталось работать примерно:')
        print(f'{how_long} секунд или {how_long / 60} минут или {how_long / 3600} часов или {how_long / 3600 / 24} дней')
        print('Введите 1, если не хотите ждать')
        a = input('Что решили? ')
        if a == "1": return True
        time_flag = False

'''бинарный поиск'''
def search(qbject, column, value):
    print('searching')
    i = 0
    j = len(qbject) - 1
    k = 0
    while 1:
        k = (j - i) // 2 + i
        if qbject.iloc[k][column] == value:
            break
        elif qbject.iloc[k][column] < value:
            i = k
        else:
            j = k
    return k

'''
формируем срез таблицы по искомому элементу (все строки)
возвращаем номера строк
'''
def slice(qbject, column, value):
    k = search(qbject, column, value) ## ищет строку с исходным элементом
    i = k
    j = k
    ## пробегаемся вверх по таблице
    while qbject.iloc[i][column] == value: 
        i -= 1
        if -1 == i: break
    ## идем вниз по таблице
    while qbject.iloc[j][column] == value:
        j += 1
        if len(qbject) == j: break
    ## возвращаем границы, в которых находится запраживаемый срез данных (строки таблицы)
    return (i + 1, j - 1)

def get_by_date(date = None, name = None, filename = None):
    global df, start_time, time_flag
    df = pd.read_csv('technical_data_analysis_using_algorithms/all_stocks_5yr.csv') ## reading csv
    time_flag = True ## just a flag
    start_time = start_time_2 = time.time() ## program started

    print('Скрипт для получения выборки по дате.') ## взаимодействие с пользователем
    val_d = ""
    while 1:
        val_d = input('Дата в формате yyyy-mm-dd [all]: ')
        if val_d in df['date'].tolist(): break
        elif val_d == "": 
            val_d = 0
            break
    val_n = ""
    while 1:
        val_n = input('Тикер [all]: ')
        if val_n in df['Name'].tolist(): break
        elif val_n == "": 
            val_n = 0
            break
    filename = ""
    while 1:
        filename = input('Файл [dump.csv]: ')
        if len(filename) >= 5 and filename[-5:-1] != '.csv': break
        elif filename == "": 
            filename = 'dump.csv'
            break
    
    start_time = time.time() ## засекаем время
    dj = poetry_get__banch(df, val_d, val_n) 
    dj.to_csv('technical_data_analysis_using_algorithms/' + filename)
    print(f'время выполнения программы - {(time.time() - start_time_2) / 60} минут')


'''сортировки, выборки, условия (все, что под капотом у скрипта)'''
def poetry_get__banch(qbject, val_d, val_n):
    date = 'date'
    name = 'Name'
    global start_time, time_flag
    if val_d == 0 and val_n == 0: ## сортировка не требуется
        return qbject

    elif val_d == 0: ## выборка только по имени
        return get_info(qbject, name, val_n)

    elif val_n == 0: ## выборка только по дате
        qbject = get_info(qbject, date, val_d)
        print(qbject)
        return qbject

    else: ## сортируем по дате и тикету
        qbject = get_info(qbject, date, val_d) ## первая сортировка-выборка
        time_flag = True ## обновляем счетчики
        start_time = time.time() ## обновляем счетчики
        qbject = get_info(qbject, name, val_n) ## вторая сортировка-выборка
        return qbject

## получаем инфо по запросу из таблицы по колонке name и значению value
def get_info(qbject, name, value):
        df_1 = quick_sort(qbject, name) ## сортировка
        a = slice(df_1, name, value) ## выборка
        if a[0] == a[1]: df_1 = df_1.iloc[a[0]] ## slice по диапазону i:1+1 почему-то возвращает пустой датафрейм
        else: df_1 = df_1.iloc[a[0]:a[1]]
        return df_1
