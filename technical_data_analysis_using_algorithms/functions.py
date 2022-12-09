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
    pd_1 = pd.DataFrame(columns = list(qbject.columns))
    pd_2 = pd.DataFrame(columns = list(qbject.columns))
    pd_2.loc[len(pd_2)] = qbject.iloc[0]
    pd_3 = pd.DataFrame(columns = list(qbject.columns))
    for i in range(1, 10000):        
        if qbject.at[i, column] < pd_2.at[0, column]:
            pd_1.loc[len(pd_1)] = qbject.loc[i]
        elif qbject.at[i, column] > pd_2.at[0, column]:
            pd_3.loc[len(pd_3)] = qbject.loc[i]
            if check(i): return cheet_sort(qbject, column) ## call sorthing with pandas
        else:
            pd_2.loc[len(pd_2)] = qbject.loc[i]
    pd_1 = pd.concat([quick_sort(pd_1, column) , pd_2])
    pd_1 = pd.concat([pd_1, quick_sort(pd_3, column)])
    return pd_1

def cheet_sort(qbject, column):
    return qbject.sort_values(by = [column])
## check how long out algo is going to work
def check(i):
    global time_flag, start_time, big_len
    if time_flag and i > 1000:
        how_time = time.time() - start_time
        big_o = big_len * math.log(big_len)
        how_long = (big_o - i) / how_time

        print(f'Алгоритм работает уже {how_time} секунд')
        print('Осталось работать примерно:')
        print(f'{how_long} секунд или {how_long / 60} минут или {how_long / 3600} часов или {how_long / 3600 / 24} дней')
        print('Введите 1, если не хотите ждать')
        a = input('Что решили? ')
        if a == "1": return True
        else: return False
        time_flag = False

df = pd.read_csv('all_stocks_5yr.csv') ## reading csv
big_len = len(df)
time_flag = True ## just a flag
start_time = time.time() ## program started
dj = quick_sort(df, 'open')
print(dj[0:3])

