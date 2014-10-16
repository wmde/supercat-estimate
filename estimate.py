#!/usr/bin/python
import os, sys
import random
import argparse
import MySQLdb
from gp import *

def parse_namespace(nsstr):
    namespaces= { "NS_MAIN": 0,
        "NS_TALK": 1,
        "NS_USER": 2,
        "NS_USER_TALK": 3,
        "NS_PROJECT": 4,
        "NS_PROJECT_TALK": 5,
        "NS_FILE": 6,
        "NS_FILE_TALK": 7,
        "NS_MEDIAWIKI": 8,
        "NS_MEDIAWIKI_TALK": 9,
        "NS_TEMPLATE": 10,
        "NS_TEMPLATE_TALK": 11,
        "NS_HELP": 12,
        "NS_HELP_TALK": 13,
        "NS_CATEGORY": 14,
        "NS_CATEGORY_TALK": 15 }
    if nsstr in namespaces:
        return namespaces[nsstr]
    try:
        return int(nsstr)
    except ValueError:
        raise Exception("invalid value for namespace: %s" % nsstr)

def get_random_article_ids(wiki, N, namespace):
    conn= MySQLdb.connect( read_default_file=os.path.expanduser("~/replica.my.cnf"), host=("%swiki.labsdb" % wiki), use_unicode=False )
    cursor= conn.cursor()
    cursor.execute("use %swiki_p" % wiki)
    ids= []
    cursor.execute( "select page_id from page where page_namespace=%%s and page_is_redirect=0 order by page_random limit %s" % N, parse_namespace(namespace) )
    res= cursor.fetchall()
    for row in res:
        ids.append(row[0])
    return ids


def print_estimate_supercategories(wiki, samples, depth, namespace):
    print("estimating super categories for namespace %s in %swiki" % (namespace, wiki))
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

    N= samples
    totalSupercats= 0
    i= 0
    uncategorized= 0
    articles= get_random_article_ids(wiki, N, namespace)
    for node in articles:
        res= conn.capture_traverse_predecessors( node, depth )
        if res: # some articles have no predecessors, resulting in None return value
            totalSupercats+= len( res )
            catset= set()
            for row in res:
                if row[0] in catset:
                    raise Exception("duplicate entry in graph processor result set: %s" % row[0])
                catset.add(row[0])
            if len(catset) != len(res):
                raise Exception("duplicate entries in graph processor result set! len(set)=%s len(result)=%s" % (len(catset), len(res)))
        else:
            uncategorized+= 1
        if i%10==0:
            sys.stdout.write('%3d%%\r' % (i*100/N))
            sys.stdout.flush()
        i+= 1
    print("%d samples processed, avg super categories: %f" % (len(articles), float(totalSupercats)/len(articles)))
    print("uncategorized nodes: %d of %d\n" % (uncategorized, len(articles)))

if __name__ == '__main__':
    parser= argparse.ArgumentParser(description= 'estimate the number of supercategories per page.', formatter_class= argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', '--namespace', default='NS_MAIN', help="namespace to check")
    parser.add_argument('-s', '--sample-size', default=1000, help="number of pages to check")
    parser.add_argument('-d', '--depth', default=9999, help="maximum supercategory depth to check")
    parser.add_argument('-w', '--wikis', default='de,en,fr', help="wikis to use")
    
    args= parser.parse_args()

    for wiki in args.wikis.split(','):
        print_estimate_supercategories(wiki, args.sample_size, args.depth, args.namespace)
    