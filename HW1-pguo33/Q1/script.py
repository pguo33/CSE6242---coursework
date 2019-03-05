import http.client
import json
import time
import sys
import collections
import csv

def main(argv):
    API_key = str(argv[1])
    # API_key = '294c183c2af2238f91da62320ff30397'
    start = time.time()

    # b. popularity
    movie_read = []
    with open('movie_id_name.csv', 'w', newline='') as csvfile:
        page = 1
        count = 0
        while count < 350:
            #API_key = '294c183c2af2238f91da62320ff30397'
            conn = http.client.HTTPSConnection("api.themoviedb.org")
            payload = "{}"
            conn.request("GET", "/3/discover/movie?api_key=" + API_key + '&sort_by=popularity.desc&page=' + str(page) +
                         '&primary_release_date.gte=2004-01-01&with_genres=18', payload)
            res = conn.getresponse()
            response = res.read().decode("utf-8")
            data = json.loads(response)
            time.sleep(0.3)
            #print(data["results"])
            for it in data['results']:
                if count == 350:
                    break
                count += 1
                movie_writer = csv.writer(csvfile)
                movie_writer.writerow([it['id'], it['title']])
                movie_read.append([it['id'], it['title']])
            page += 1

    # c. step 1: similar movie retrieval
    similar = []
    for row in movie_read:
        # print(type(movie_reader))
        # print(row)
        mid = row[0]
        conn = http.client.HTTPSConnection("api.themoviedb.org")
        payload = "{}"
        conn.request("GET", '/3/movie/' + str(mid) + '/similar?api_key=' + API_key, payload)
        res = conn.getresponse()
        response = res.read().decode("utf-8")
        data = json.loads(response)
        time.sleep(0.3)
        if data['total_results'] >= 5:
            counts = 5
        else:
            counts = data['total_results']
            # print(data['total_results'])
        if counts > 0:
            for index in range(counts):
                similar.append([str(mid), str(data['results'][index]['id'])])

    # c. step 2: deduplication
    deline = []
    for it1 in similar:
        for it2 in similar:
            if it1 == [it2[1], it2[0]]:
                if int(it1[0]) > int(it2[0]):
                    deline.append(similar.index(it1))
                else:
                    deline.append(similar.index(it2))

    for i in sorted(deline, reverse=True):
        del similar[i]

    with open('movie_ID_sim_movie_ID.csv', 'w', newline='\n') as csvfileSim:
        write = csv.writer(csvfileSim)
        write.writerow(['Source', 'Target'])
        write.writerows(similar)

    end = time.time()
    print(end - start)


if __name__ == "__main__":
    print(sys.argv)
    main(sys.argv)

