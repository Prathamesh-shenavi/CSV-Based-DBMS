import os
import csv
import re
import os.path
import pandas as pd
import glob
import openpyxl
from openpyxl.utils import FORMULAE
import string

file_names=[]
words=[]
row=[]
line=[]
primary_key=[]
idx=0
def create_table(words):

    s=" "
    s=s.join(words)
    s = re.sub("[()]", "", s)
    s = re.sub("[,]", "", s)
    w=s.split()
    #print(s)
    flag=check_file()
    #print(flag)
    if flag!=0:
        # print("\ncreating table " + w[2])
        # print(words)

        Fname = w[2]
        file_names.append(Fname)
        #print(file_names)
        attri = []
        for abc in w:
            if abc =="Primary" or abc=="PRIMARY":
                p_key=True
                break
            else:p_key=False
        l = len(w)
        # print(w)
        # print(l)
        for i in range(l):
            # print(w[i])
            if w[i] == "INT" or w[i] == "STR":
                attri.append(w[i - 1])
            if (w[i].upper() == "PRIMARY" and w[i + 1].upper() == "KEY"):
                #print("\nin key")
                if w[i+2] in attri:
                    primary_key.append(w[i+2])

                    #w.remove(w[i])
                    #w.remove(w[i+1])
        key=[]
        key.append(w[2])
        key.append(primary_key[0])
        # print(key)
        with open('pk.csv', 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows([key])




        #print(primary_key)
        # print(attri)
        if p_key:
            with open(Fname + ".csv", mode='w') as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=attri)
                writer.writeheader()
            df = pd.read_csv(words[2] + '.csv')
            #for i in primary_key:
                #idx = df.columns.get_loc(w[i + 2])
            w.remove("CREATE")
            w.remove("TABLE")
            b=' '.join(map(str,w))
            b=" # ".join(b.split())
            file1 = open("Schema.txt", "a")
            file1.write(b+"\n")
            file1.close()
            print("\nTable "+w[0]+" created successfully ")

        else:
            print("Primary key is not defined\n")
    else:
        print("\nTable is already exist in files !!")

def pk_search(w):
    file = open("pk.csv")
    csvreader = csv.reader(file)
    header = next(csvreader)
    # print(header)
    idx = header.index('NAME')
    id_key=header.index('PK')
    rows = []
    for row in csvreader:
        try:
            if w in row[idx]:
                data_fnd = 1
                primary_key.append(row[id_key])
                # print(row)
            #rows.append(row[])
        except:
            continue
    # print(rows)
    #

def insert_table(line):
    attri = words[4]
    attri = re.sub("[()]", "", attri)
    attri = re.sub("[;]", "", attri)
    attri = attri.split(',')
    rows=[attri]
    row.append(rows)
    #print(rows)
    flag = check_file()

    #print(total_columns)
    if flag==0:
        total_columns = check_no_columns()
        if total_columns==len(attri):
            #pk_search(words[2])
            file = open(words[2]+".csv")
            csvreader = csv.reader(file)
            header = next(csvreader)
            pk_search(words[2])
            idx = header.index(primary_key[0])
            #print(idx)
            row_ins=[]
            for i in csvreader:
                try:
                    row_ins.append(i[idx])
                except:
                    continue
            #print(attri[idx])
            if attri[idx] in row_ins:
                print("\nPrimary key is already exist !!!")
            else:
                with open(words[2]+'.csv', 'a', newline='') as csv_file:
                    writer = csv.writer(csv_file)
                    writer.writerows(rows)
                print("\ninserted in "+words[2]+" !!")
        else:
            print("\nAtrribute number is not matching !!")
    else:
        print("\n"+words[2]+" table is not in database to insert any row !!")

def delete_table(words):
    #print("deleting")
    flag = check_file()
    if flag==0:

        #memberName = words[2]
        os.remove(words[2] + ".csv")
        #file_names.remove(words[2])
        c="".join(words[2])
        #print(c)
        print("\n"+words[2]+" table is Deleted !!")
        with open("Schema.txt", "r") as input:
            with open("bb.txt", "w") as output:
                for line in input:
                    if c not in line.strip("\n"):
                        output.write(line)
        os.replace('bb.txt', 'Schema.txt')
        file='pk.csv'
        column_name='NAME'
        row_to_remove = words[2]
        sign='='
        #print(row_to_remove)
        try:
            remove_row_equalto(file,column_name,words[2],sign)
            # df = pd.read_csv(file)
            # for row in row_to_remove:
            #     df = df[eval("df.{}".format(column_name)) == row]
            # df.to_csv(file, index=False)
            #print("DELETED..!!")
        except Exception as e:
            raise Exception("Error message....")




    else:
        print("\n"+words[2]+" table is not in database !!")

def remove_row_equalto(file, column_name, args, sign):
    row_to_remove = []
    flag=args.isdigit()
    #print(type(args))
    if flag:
        s = int(args)
    else:
        s=args

    #for row_name in s:
    row_to_remove.append(s)
    #print(row_to_remove)

    try:
        df = pd.read_csv(file)
        if sign=='=':
            for row in row_to_remove:
                df = df[eval("df.{}".format(column_name))  != row]
            df.to_csv(file, index=False)
            print("DELETED..!!")
        elif sign == '!=':
            for row in row_to_remove:
                df = df[eval("df.{}".format(column_name))  == row]
            df.to_csv(file, index=False)
            print("DELETED..!!")
        elif sign == '>=':
            for row in row_to_remove:
                df = df[eval("df.{}".format(column_name))  < row]
            df.to_csv(file, index=False)
            print("DELETED..!!")
        elif sign == '>':
            for row in row_to_remove:
                df = df[eval("df.{}".format(column_name))  <= row]
            df.to_csv(file, index=False)
            print("DELETED..!!")
        elif sign == '<=':
            for row in row_to_remove:
                df = df[eval("df.{}".format(column_name))  > row]
            df.to_csv(file, index=False)
            print("DELETED..!!")
        elif sign == '<':
            for row in row_to_remove:
                df = df[eval("df.{}".format(column_name))  >= row]
            df.to_csv(file, index=False)
            print("DELETED..!!")
        else:print("\nSomthing is not correct ..Try Again !!")
    except Exception as e:
        raise Exception("Error message....")

def delete_from(words):
    # delete from std where ID = 1
    # delete from std where NAME = A
    # delete from std
    #delete from std
    #print("deleting")
    #print(words)
    flag=check_file()
    if flag==0:

        for i in words:
            if i.upper()=='WHERE':
                flag_whr=1
                break
            else:flag_whr=0
        if flag_whr==1:
            file = words[2] + '.csv'
            attri=[words[4]]
            column_name = words[4]
            #print(column_name)
            flag_attri=check_attri_in_table(words[2],attri)
            if flag_attri==0:
                remove_row_equalto(file,column_name,words[6],words[5])

            else:
                print("\nAttribute is not there in "+words[2]+" table")
        else:
            print("\nAll Data is deleted from "+ words[2] +' !!')
            with open(words[2]+'.csv') as inp:
                data_in = inp.readlines()
            n = 0
            with open(words[2]+'.csv', 'w') as outfile:
                outfile.writelines(data_in[:n + 1])

    else:
        print("\nTable Doesn't Exist !!")

def select_where(words,tb_name):
    attri_wh = []
    for i in range(len(words)):
        if words[i].upper() == 'WHERE':
            break
    attri_wh.extend(words[i + 1:])
    # print(attri_wh)
    for i in range(len(attri_wh)):
        if attri_wh[i] == '=':
            choice = 1
            col = []
            col.append(attri_wh[i - 1])
            # print(col)
            search = attri_wh[i + 1]
            # print(search)
        elif attri_wh[i] == '<=':
            choice = 2
            col = []
            col.append(attri_wh[i - 1])
            # print(col)
            search = attri_wh[i + 1]
            # print(search)
        elif attri_wh[i] == '>=':
            choice = 3
            col = []
            col.append(attri_wh[i - 1])
            # print(col)
            search = attri_wh[i + 1]
            # print(search)
        elif attri_wh[i] == '<':
            choice = 4
            col = []
            col.append(attri_wh[i - 1])
            # print(col)
            search = attri_wh[i + 1]
            # print(search)
        elif attri_wh[i] == '>':
            choice = 5
            col = []
            col.append(attri_wh[i - 1])
            # print(col)
            search = attri_wh[i + 1]
            # print(search)
        elif attri_wh[i] == '!=':
            choice = 6
            col = []
            col.append(attri_wh[i - 1])
            # print(col)
            search = attri_wh[i + 1]
            # print(search)
    flag_at = check_attri_in_table(tb_name, col)

    # print(type(search))
    flag_is_int = search.isdigit()
    if flag_is_int:
        srh = int(search)
    else:
        srh = search
    if flag_at == 0:
        # print("\nAttri is there")
        file = open(tb_name + ".csv")
        csvreader = csv.reader(file)
        header = next(csvreader)
        idx = header.index(col[0])
        data_fnd = 0

        # for ==
        for row in csvreader:
            if len(row) != 0:
                if flag_is_int:
                    s = int(row[idx])
                else:
                    s = row[idx]
                if choice == 1:
                    if s == srh:
                        data_fnd = 1
                        print(row)
                elif choice == 2:

                    if s <= srh:
                        data_fnd = 1
                        print(row)
                elif choice == 3:

                    if s >= srh:
                        data_fnd = 1
                        print(row)
                elif choice == 4:

                    if s < srh:
                        data_fnd = 1
                        print(row)
                elif choice == 5:
                    if s > srh:
                        data_fnd = 1
                        print(row)
                elif choice == 6:
                    if s != srh:
                        data_fnd = 1
                        print(row)
                else:
                    print("\nSomthing is wrong !! Try Again..")

                # print(row[idx])

        if data_fnd == 1:
            print("\nData found")
        else:
            print("\nData not Found")
    elif flag_at == 1:
        print("\nInvalid Attributes in SELECT !!")

def select(words):
    #print("in select\n")
    #SELECT * FROM std WHERE ID = 1
    #select * from std where NAME = A
    #select * from std where NAME='A' AND ID=1
    #select NAME from std
    #select NAME,ID from std
    #select NAME from std where ID = 2
    #select NAME,ID from std where ID = 1
    t_attri=[]
    for i in range(len(words)):
        if words[i].upper()=="FROM":
            t_attri.append(words[i-1])
    #print(t_attri)
    attri = []
    attri = t_attri[0].split(',')
    # print(attri)
    for i in words:
        if i.upper()=='WHERE':
            wh_flag=0
            break
        else:wh_flag=1
    for i in range(len(words)):
        if words[i].upper()=="FROM":
            tb_name=words[i+1]
            break
    file_exist=os.path.exists(tb_name+'.csv')
    if file_exist:
        #print("Table is there\n")
        data = pd.read_csv(tb_name + ".csv")

        #* without where
        if words[1]=="*" and wh_flag==1:
            # print("\n* without where")
            print("Table: "+tb_name)
            print(data)
            #print(rows)

        # * with where
        elif words[1]=="*" and wh_flag==0:
            select_where(words,tb_name)


        #attri without where
        elif wh_flag==1:
            #print("\nattri without where clouse ")
            flag_attri=check_attri_in_table(tb_name,attri)

            if flag_attri==0:
                #print("\nAttri is there")

                for i in attri:
                    print(i)
                    print(data[i].tolist())
            elif flag_attri==1:
                print("\nInvalid Attributes in SELECT !!")

        #attri with where
        elif wh_flag==0:
            # print("\nattri with where clouse ")
            flag_attri = check_attri_in_table(tb_name, attri)

            if flag_attri == 0:
                # print("\nAttri is there")
                attri_wh = []
                for i in range(len(words)):
                    if words[i].upper() == 'WHERE':
                        break
                attri_wh.extend(words[i + 1:])
                # print(attri_wh)
                for i in range(len(attri_wh)):
                    if attri_wh[i] == '=':
                        choice = 1
                        col = []
                        col.append(attri_wh[i - 1])
                        # print(col)
                        search = attri_wh[i + 1]
                        # print(search)
                    elif attri_wh[i] == '<=':
                        choice = 2
                        col = []
                        col.append(attri_wh[i - 1])
                        # print(col)
                        search = attri_wh[i + 1]
                        # print(search)
                    elif attri_wh[i] == '>=':
                        choice = 3
                        col = []
                        col.append(attri_wh[i - 1])
                        # print(col)
                        search = attri_wh[i + 1]
                        # print(search)
                    elif attri_wh[i] == '<':
                        choice = 4
                        col = []
                        col.append(attri_wh[i - 1])
                        # print(col)
                        search = attri_wh[i + 1]
                        # print(search)
                    elif attri_wh[i] == '>':
                        choice = 5
                        col = []
                        col.append(attri_wh[i - 1])
                        # print(col)
                        search = attri_wh[i + 1]
                        # print(search)
                    elif attri_wh[i] == '!=':
                        choice = 6
                        col = []
                        col.append(attri_wh[i - 1])
                        # print(col)
                        search = attri_wh[i + 1]
                        # print(search)
                flag_at = check_attri_in_table(tb_name, col)

                # print(type(search))
                flag_is_int = search.isdigit()
                if flag_is_int:
                    srh = int(search)
                else:
                    srh = search
                if flag_at == 0:
                    # print("\nAttri is there")
                    file = open(tb_name + ".csv")
                    csvreader = csv.reader(file)
                    header = next(csvreader)
                    idx_list = []
                    idx = header.index(col[0])
                    for i in attri:
                        id = header.index(i)
                        idx_list.append(id)
                    data_fnd = 0

                    # for ==
                    for row in csvreader:
                        if len(row) != 0:
                            if flag_is_int:
                                s = int(row[idx])
                            else:
                                s = row[idx]
                            if choice == 1:
                                if s == srh:
                                    data_fnd = 1
                                    print_content(row,idx_list)
                            elif choice == 2:

                                if s <= srh:
                                    data_fnd = 1
                                    print_content(row,idx_list)
                            elif choice == 3:

                                if s >= srh:
                                    data_fnd = 1
                                    print_content(row,idx_list)
                            elif choice == 4:

                                if s < srh:
                                    data_fnd = 1
                                    print_content(row,idx_list)
                            elif choice == 5:
                                if s > srh:
                                    data_fnd = 1
                                    print_content(row,idx_list)
                            elif choice == 6:
                                if s != srh:
                                    data_fnd = 1
                                    print_content(row,idx_list)
                            else:
                                print("\nSomthing is wrong !! Try Again..")

                            # print(row[idx])

                    if data_fnd == 1:
                        print("\nData found")
                    else:
                        print("\nData not Found")
                elif flag_at == 1:
                    print("\nInvalid Attributes in SELECT !!")

            elif flag_attri == 1:
                print("\nInvalid Attributes in SELECT !!")
        else:
            print("\nInvalid SELECT statement !!!")
    else:
        print("\nTable is not there in DATABASE !!!")
    #print(words)
    #print(tb_name)

def print_content(row,idx_list):
    content = list(row[i] for i in idx_list)
    print(content)

def check_attri_in_table(tb_name,attri):
    with open(tb_name + ".csv", 'r') as file:
        csvreader = csv.reader(file)
        header = next(csvreader)
    #print(header)
    for i in attri:
        if i in header:
            flag_attri = 0
        else:
            flag_attri = 1
            break
    return flag_attri

def update(words):
    #UPDATE std SET NAME = ABC where ID = 1
    #UPDATE std SET ID = 3 where NAME = B
    #UPDATE std SET NAME = ABC , ID = 2 where ID = 1
    #UPDATE std SET NAME = ABC
    #UPDATE std SET ID = 1
    flag_tb =os.path.exists(words[1]+'.csv')
    attri=[]
    search=[]
    if flag_tb:
        #print("\nTable exists")
        for i in words:
            if i.upper()=='WHERE':
                flag_whr=1
                break
            else:flag_whr=0

        #print(attri_wh)
        for i in range(len(words)):
            if words[i]=='=':
                attri.append(words[i-1])


        #print(attri)
        flag_attri=check_attri_in_table(words[1],attri)
        if flag_attri==0:
            if flag_whr:
                with open('pk.csv', newline='') as csvfile:
                    data = csv.DictReader(csvfile)
                    for row in data:
                        if row['NAME'] == words[1]:
                            key = row['PK']
                att=[]
                #print("with where")
                for i in range(len(words)):
                    if words[i].upper()=='SET':
                        att.extend(words[i+1:i+4])
                #print(att)

                attri_wh = []
                for i in range(len(words)):
                    if words[i].upper() == 'WHERE':
                        break
                attri_wh.extend(words[i + 1:])
                #print(attri_wh)
                if key==att[0]:
                    file = open(words[1] + ".csv")
                    csvreader = csv.reader(file)
                    header = next(csvreader)
                    pk_search(words[1])
                    idx = header.index(primary_key[0])
                    # print(header)
                    row_ins = []
                    for i in csvreader:
                        try:
                            row_ins.append(i[idx])
                        except:
                            continue
                    #print(att[idx+2])
                    #print(row_ins)
                    if att[idx+2] in row_ins:
                        print("\nPrimary key is already exist !!!")
                    else:
                        rep = att[2]
                        s = attri_wh[2]
                        file = words[1]+'.csv'
                        flag = s.isdigit()
                        if flag:
                            srh = int(s)
                        else:
                            srh = s
                        df = pd.read_csv(words[1]+'.csv')
                        df.loc[df[attri_wh[0]] == srh, attri[0]] = rep
                        print(df)
                        df.to_csv(file, index=False)
                else:
                    rep = att[2]
                    s = attri_wh[2]
                    file = words[1] + '.csv'
                    flag = s.isdigit()
                    if flag:
                        srh = int(s)
                    else:
                        srh = s
                    df = pd.read_csv(words[1] + '.csv')
                    df.loc[df[attri_wh[0]] == srh, attri[0]] = rep
                    print(df)
                    df.to_csv(file, index=False)
            else:
                file=words[1]+'.csv'
                #print('without where')

                for i in range(len(words)):
                    if words[i]=='=':
                        find=words[i+1]

                with open('pk.csv', newline='') as csvfile:
                    data = csv.DictReader(csvfile)
                    for row in data:
                        if row['NAME'] == words[1]:
                            key = row['PK']
                #print(key)
                if attri[0] != key:
                    df = pd.read_csv(words[1]+'.csv')
                    df.loc[df[attri[0]] != find, attri[0]] = find
                    df.to_csv(file, index=False)
                    print(df)
                else:
                    print("\nIts an Primary key")


        else:print("\nAttributes are not in Table..")

    else:print("\ntable is not in Database..!!")
    #print(words)

def detect_mn(line):
    global words
    if (words[0].upper() =='CREATE' and words[1].upper()=='TABLE'):
        create_table(line)
    elif(words[0].upper()=='DROP' and words[1].upper()=='TABLE'):
        delete_table(words)
    elif(words[0].upper()=='INSERT' and words[1].upper()=='INTO' and len(words)>=5):
        insert_table(words)
    elif(words[0].upper()=='HELP' and len(words)==1):
        help()
    elif (words[0].upper() == 'HELP' and words[1].upper() =='DESCRIBE'):
        help_describe(words)
    elif(words[0].upper()=='HELP' and words[1].upper()=='TABLES' ):
        help_tables(words)
    elif(words[0].upper()=="SELECT" and len(words)>=4):
        for i in range(len(words)):
            if words[i]=="from" or words[i]=="FROM":

                flag=1
                break
            else:
                flag=0

        if flag:
            select(line)
        else:print("\n########  INVALID QUERY  ##########\n          TRY AGAIN...!!")
    elif(words[0].upper()=='DELETE' and len(words)>=3):
        for i in range(len(words)):
            if words[i].upper()=="FROM":
                flag=1
                break
            else:flag=0
        if flag:
            delete_from(words)
        else:print("\n########  INVALID QUERY  ##########\n          TRY AGAIN...!!")
    elif (words[0].upper()=='UPDATE' ):
        for i in range(len(words)):
            if words[i].upper() == "SET":
                flag = 1
                break
            else:
                flag = 0
        if flag:
            update(words)
        else:
            print("\n########  INVALID QUERY  ##########\n          TRY AGAIN...!!")

    else:
        print("\n########  INVALID QUERY  ##########\n          TRY AGAIN...!!")

def remove(list):
    pattern = '[0-9]'
    list = [re.sub(pattern, '', i) for i in list]
    a = " "

    # return string
    return (a.join(list))

def check_file():
    flag=1
    file_exists = os.path.exists(words[2]+'.csv')

    #print(file_exists)
    if file_exists:
        return 0
    else:
        return 1

def help_tables(words):
    print("\nTables in database:")
    path = os.getcwd()
    csv_files = glob.glob(os.path.join(path, "*.csv"))
    for f in csv_files:
        # read the csv file
        #df = pd.read_csv(f)

        # print the location and filename
        #print('Location:', f)
        f=f.split("\\")[-1]
        f=f.split(".")[0]
        print(f)

def help_describe(words):
    print("\nTable info:")
    c = "".join(words[2])
    # print(c)
    with open('Schema.txt', 'r') as file:
        while True:
            line=file.readline()
            if not line:
                break
            sch=line.split(" ")
            if sch[0]==words[2]:
                print(line)
                break

def check_no_columns():
    flag=check_file()
    if flag==0:
        df=pd.read_csv(words[2]+'.csv')
        total_rows=len(df.axes[0])
        total_cols = len(df.axes[1])
        #print(total_rows)
    return  (total_cols)

def help():
    print("\n->CREATE TABLE\n")
    print(" Example: \n CREATE TABLE std ( ID INT, Primary key (ID) , NAME STR )  ")
    print("\n->DROP TABLE\n")
    print(" Example: \n DROP TABLE std")
    print("\n->INSERT INTO TABLE\n")
    print(" Example: \n INSERT INTO std VALUES (1,A)")
    print("\n->DELETE\n")
    print(" Example: \n 1. delete from std where ID = 1 \n 2. delete from std")
    print("\n->SELECT\n")
    print(" Example: \n SELECT * FROM std \n SELECT * FROM std WHERE ID = 1 \n select * from std where NAME='A' AND ID=1 \n select NAME from std \n select NAME,ID from std where ID = 1")



ch=0
line=[]
a=" "
print("\n======= WELCOME TO PS SQL ========")
while ch!=1:
    line = [input("\n$ Enter Your Query: ")]
    #print(type(line))
    a=a.join(line)

    words = a.split()

    #print(words)
    if words[0].upper()=="QUIT":
        ch=1
        print("\n#####################################\n")
        print("^_^ Thank You for using our system !!")
        print("\n#####################################")
        break
    detect_mn(words)
'''
     CREATE TABLE std ( ID INT, Primary key (ID) , NAME STR )
     
     INSERT INTO std VALUES (1,A)
     INSERT INTO std VALUES (2,A)
     INSERT INTO std VALUES (3,B)
     
     SELECT * FROM std WHERE ID = 1
     SELECT * FROM std
     SELECT * FROM std WHERE ID = 1
     SELECT * from std where NAME = A
     SELECT NAME from std
     SELECT NAME,ID from std
     SELECT NAME from std where ID = 2
     SELECT NAME,ID from std where ID = 1
     
     DELETE from std where ID = 1
     DELETE from std
     
     UPDATE std SET NAME = ABC where ID = 1
     UPDATE std SET ID = 3 where NAME = B
     UPDATE std SET NAME = ABC
     UPDATE std SET ID = 1
     
     DROP TABLE std
     
     HELP DESCRIBE std
     
     HELP
 '''





