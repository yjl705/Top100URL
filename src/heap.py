import heapq

def main():
    f = open("../tmp/totalResult.txt","r")
    heap = []
    for i in range(100):
        line = f.readline()
        space = line.rfind(" ")
        URL, time = line[:space], line[space+1:]
        time = int(time)
        if line:
            heapq.heappush(heap, [URL, time])
        else:
            break
    while True:
        line = f.readline()
        if line:
            space = line.rfind(" ")
            URL, time = line[:space], line[space + 1:]
            time = int(time)
            if time > heap[0][1]:
                heapq.heappop(heap)
                heapq.heappush(heap, [URL, time])
        else:
            break

    f.close()

    fo = open("../tmp/Result.txt","a")
    heap.sort(key = lambda k : k[1], reverse = True)
    for k,v in heap:
        fo.write(str(k) + " " + str(v) + "\n")

if __name__ == '__main__':
    main()