import os, sys
import random
import MySQLdb
from gp import *


def get_random_article_ids(wiki, N):
    conn= MySQLdb.connect( read_default_file=os.path.expanduser("~/replica.my.cnf"), host=("%swiki.labsdb" % wiki), use_unicode=False )
    cursor= conn.cursor()
    cursor.execute("use %swiki_p" % wiki)
    ids= []
    cursor.execute("select page_id from page where page_namespace=0 and page_is_redirect=0 order by page_random limit %s" % N)
    res= cursor.fetchall()
    for row in res:
        ids.append(row[0])
    return ids


def print_estimate_supercategories(wiki):
    print("estimating super categories per article for %swiki" % wiki)
    conn= client.Connection(client.ClientTransport("sylvester", 6666))
    conn.strictArguments= False
    conn.connect()
    conn.use_graph("%swiki" % wiki)
    
    stats= conn.capture_stats()
    for line in stats:
        if line[0]=='MinNodeID':
            MinNodeID= int(line[1])
        elif line[0]=='MaxNodeID':
            MaxNodeID= int(line[1])

    N= 10000
    totalSupercats= 0
    i= 0
    uncategorizedArticles= 0
    articles= get_random_article_ids(wiki, N)
    for node in articles:
        res= conn.capture_traverse_predecessors( node, 0xFFFFFFF0 )
        if res: # some articles have no predecessors, resulting in None return value
            totalSupercats+= len( res )
        else:
            uncategorizedArticles+= 1
        if i%10==0:
            sys.stdout.write('%3d%%\r' % (i*100/N))
            sys.stdout.flush()
        i+= 1
    print("%d leaf samples processed, avg super categories per leaf: %f" % (len(articles), float(totalSupercats)/i))
    print("uncategorized articles: %d of %d\n" % (uncategorizedArticles, len(articles)))

if __name__ == '__main__':
    for wiki in [ 'de', 'en', 'fr' ]:
        print_estimate_supercategories(wiki)
    