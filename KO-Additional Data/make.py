import csv

def normalize_to_json(string):
    return '[' + string.replace('}{', '},{') + ']'

biochemicals = list(csv.DictReader(open('BIONET Key Organics Biochemicals GHS data and Image URL.csv', 'r')))
building_blocks = list(csv.DictReader(open('BIONET Key Organics Building Blocks GHS data and Image URL.csv', 'r')))
screeing = list(csv.DictReader(open('BIONET Key Organics Screening & Fragments GHS data and Image URL.csv', 'r')))
biochemicals_s = [item['ID'] for item in biochemicals]
building_blocks_s = [item['ID'] for item in building_blocks]
screeing_s = [item['ID'] for item in screeing]

products_new = []

with open('result.csv', 'r') as csvfile:
    def find(id, handler, handler2):
        for i, r in enumerate(handler):
            if id == r:
                d = dict(handler2[i])
                del d['ID']
                return d

    counter = 0
    next_i = 0
    reader = list(csv.DictReader(csvfile))
    for i in range(len(reader) + 1):
        i += next_i
        row = reader[i]
        counter += 1
        if counter % 10000 == 0:
            print(str(counter))
        id = '-'.join(row['ID'].split('-')[0:2])
        next_dict = find(id, biochemicals_s, biochemicals)
        if next_dict:
            row.update(next_dict)
            products_new.append(row)
            for j in range(i + 1, i + 10):
                if id in reader[j]:
                    new_row = reader[j]
                    new_row.update(next_dict)
                    products_new.append(new_row)
                else:
                    next_dict += j
            continue
        next_dict = find(id, building_blocks_s, building_blocks)
        if next_dict:
            row.update(next_dict)
            products_new.append(row)
            continue
        next_dict = find(id, screeing_s, screeing)
        if next_dict:
            row.update(next_dict)
            products_new.append(row)
            for j in range(i, i + 10):
                if id in reader[j]:
                    new_row = reader[j]
                    new_row.update(next_dict)
                    products_new.append(new_row)
                else:
                    next_dict += j


with open('result_2.csv', 'a') as csvfile:
    fieldnames = list(products_new[0].keys())
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for product in products_new:
        writer.writerow(product)