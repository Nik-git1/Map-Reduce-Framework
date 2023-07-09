mod=0
red=1
with open("50001.txt",'r') as f:
    content_list = f.readlines()

# print the list

# remove new line characters
content_list = [x.strip() for x in content_list]
# print(content_list)

dic={}
for str in content_list:
    n_l=str.split()
    if((len(n_l[0])%red)==mod):
        if not n_l[0] in dic.keys():
            dic[n_l[0]]={}

        for i in range(1,len(n_l)):
            temp=n_l[i].split(':')
            tk=temp[0][1:]
            tv=temp[1][:-1]
            if not tk in dic[n_l[0]]:
                dic[n_l[0]][tk]=[]
            dic[n_l[0]][tk].append(tv)

with open("50002.txt",'r') as f:
    content_list2 = f.readlines()

# print the list

# remove new line characters
content_list2 = [x.strip() for x in content_list2]
for str in content_list2:
    n_l=str.split()
    if((len(n_l[0])%red)==mod):
        if not n_l[0] in dic.keys():
            dic[n_l[0]]={}

        for i in range(1,len(n_l)):
            temp=n_l[i].split(':')
            tk=temp[0][1:]
            tv=temp[1][:-1]
            if not tk in dic[n_l[0]]:
                dic[n_l[0]][tk]=[]
            dic[n_l[0]][tk].append(tv)
final_dic={}
for k, v in dic.items():
    final_dic[k]=[]
    for ik, iv in dic[k].items():
        final_dic[k].append(iv)
final_list=[]
for fk,fv in final_dic.items():
    if len(fv)==2:
        list1=fv[0]
        list2=fv[1]
        for age in list1:
            for role in list2:
                temp_str=fk+" " + age+", " + role
                final_list.append(temp_str)

print(final_list)
