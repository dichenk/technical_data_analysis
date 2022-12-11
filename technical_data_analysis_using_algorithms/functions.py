import csv
import pandas as pd
import time
import math
'''
## декоратор для функций, который считает количество вызовов функции (для соответствующего имени файла)
def decor_count(func):
    def wrapper(*args, **kwargs):
        wrapper.counter += 1
        return func(*args, **kwargs)
    wrapper.counter = 0
    return wrapper

## функция кэширования запроса
def cash_function(cashed_arguements, amount_of_calling):
    try:
        with open('cash.json', 'r') as my_favourite_json:
            data = json.load(my_favourite_json)
    except:
        data = {}
    try: 
        ## ищем запрос в кэшэ запросов
        return data[cashed_arguements]
    except: 
        ## записываем запрос в кэш запросов
        data[cashed_arguements] = amount_of_calling
        with open('cash.json', 'w') as my_favourite_json:
            json.dump(data, my_favourite_json)
        return 0

## Функция выполнения сортировки и фильтра
def select_sorted_backdoor(sort_columns, limit, order):
    dv = vaex.from_csv(file_path, convert = True)
    dv = dv[dv.Name == order]
    dv = dv.sort(sort_columns, ascending = False)
    dv  = dv[0:limit]
    return dv

@decor_count
def select_sorted(sort_columns_f = sort_columns , limit_f = limit, group_by_name_f = group_by_name, order_f = order):
    a = ['select_sorted_backdoor', sort_columns_f, str(limit_f), str(group_by_name_f), order_f]
    a = ','.join(a)
    cash_fu = cash_function(a, select_sorted.counter)
    if cash_fu == 0:
        ## Вызов функции впервые. Вызываем, обрабатываем, кэшируем результат
        b = select_sorted_backdoor(sort_columns_f, limit_f, order_f)
        b.export_hdf5(str(select_sorted.counter) + '.hdf5')
        b.export_csv(filename + str(select_sorted.counter) + filename_type)
    else:
        ## Функцию не вызываем, достаем инфу из кэша
        b = vaex.open(str(cash_fu) + '.hdf5')
        b.export_csv(filename + str(select_sorted.counter) + filename_type)


def get_by_date(date = date_sec, name=name_sec):
    ## одна из функций домашней работы
    dv = vaex.from_csv(file_path, convert=True)
    dvv = dv[dv.Name == name]
    dvv = dvv[dv.date == date]
    dvv.export_csv(filename + filename_type)

## Функция к домашке по 25 дню
def get_by_date2(date = date_sec, name = name_sec):

    a = []
    with open('all_stocks_5yr.csv', "r") as file:
        reader = csv.reader(file)
        print(reader)
        for i in reader:
            if i[0] == date and i[6] == name
                a.append(i)

    with open(filename2, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(a)
'''
## to show that I can rwite down an algo
def quick_sort(qbject, column):
    ## делим будем делить таблицу на три части
    pd_1 = pd.DataFrame(columns = list(qbject.columns)) ## больше искомого элемента
    pd_2 = qbject.iloc[[0]] ## равно
    pd_3 = pd.DataFrame(columns = list(qbject.columns)) ## меньше

    if len(qbject) == 1: return qbject ## выход из рекурсии

    for i in range(len(qbject)):
        print(i)
        if qbject.iloc[i][column] < qbject.iloc[0][column]:
            pd_1 = pd.concat([pd_1, qbject.iloc[[i]]])
        elif qbject.iloc[i][column] > qbject.iloc[0][column]:
            pd_3 = pd.concat([pd_3, qbject.iloc[[i]]])
            if check(i, qbject): 
                return cheet_sort(qbject, column) ## call sorthing with pandas
        else:
            pd_2 = pd.concat([pd_2, qbject.iloc[[i]]])
    try:
        pd_1 = pd.concat([quick_sort(pd_1, column), pd_2])
    except:
        pd_1 = pd.concat([pd_1, pd_2])
    try:
        pd_1 = pd.concat([pd_1, quick_sort(pd_3, column)])
    except:
        pd_1 = pd.concat([pd_1, pd_3])
    return pd_1

def cheet_sort(qbject, column):
    print(9)
    qbject = qbject.sort_values(by = column)
    qbject.to_csv('cheet.csv')
    return qbject

## check how long out algo is going to work
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

## binary searsh
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

def slice(qbject, column, value):
    k = search(qbject, column, value)
    i = k
    j = k
    while qbject.iloc[i][column] == value:
        i -= 1
        if -1 == i: break
    while qbject.iloc[j][column] == value:
        j += 1
        if len(qbject) == j: break
    return (i + 1, j - 1)

def get_by_date(qbject = None, date = 0, name = 0, val_d = 0, val_n = 0):
    if date == 0 and name == 0:
        return qbject
    elif date == 0:
        return get_info(qbject, name, val_n)
    elif name == 0:
        qbject = get_info(qbject, date, val_d)
        print(qbject)
        return qbject
    else:
        qbject = get_info(qbject, date, val_d)
        print(qbject)
        time_flag = True
        start_time = time.time()
        qbject = get_info(qbject, name, val_n)
        print(qbject)
        return qbject

def get_info(qbject, name, value):
        df_1 = quick_sort(qbject, name)
        a = slice(df_1, name, value)
        if a[0] == a[1]: df_1 = df_1.iloc[a[0]] ## slice по диапазону i:1+1 почему-то возвращает пустой датафрейм
        else: df_1 = df_1.iloc[a[0]:a[1]]
        return df_1

df = pd.read_csv('all_stocks_5yr.csv') ## reading csv
time_flag = True ## just a flag
start_time = start_time_2 = time.time() ## program started
dj = get_by_date(df, 'date', "Name", '2016-06-14', 'A')
dj.to_csv('result.csv')

print(dj)
print(f'время выполнения программы - {(time.time() - start_time_2) / 60} минут')
