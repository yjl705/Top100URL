
def hashURL(URL):
    return hash(URL) % 1000


def main():
    print("start")
    f = open("../data/data100mb.txt","r") #先用最小文件测试
    fs = []
    for i in range(1000):
        fs.append(open("../tmp/hashFile/" + str(i) +".txt", "a"))
    while True:
        line = f.readline()
        if line:
            hashVal = hashURL(line)
            fs[hashVal].write(line+"\n")
        else:
            break
    f.close()
    for i in range(1000):
        fs[i].close()



if __name__ == '__main__':
    main()