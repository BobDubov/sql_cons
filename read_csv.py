import csv

with open("20211011.CSV", encoding='cp1251') as file:
    reader = csv.reader(file, delimiter=';')

for line in reader:
    print(line[0])
#     if line[0] != 'Дата пополнения' and line[3] not in base_name_list:
#         new_base_list.append((line[2], line[3]))
#
# print(reader)
