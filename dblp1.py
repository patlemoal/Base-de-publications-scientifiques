from pymongo import MongoClient
from pprint import pprint


client = MongoClient("mongodb://localhost:27017/")
publis = client.dblp.publis

# Compter le nombre de documents de la collection publis;
def count_docs():
    doc_count = publis.find().count()

    print(f"le nombre de documents en base est {doc_count}.")



# Lister tous les livres (type “Book”)
def list_books():
    list_books = list(publis.find({"type" : "Book"}, {"_id" :0, "title": 1}))
    #pprint(list_books)
    print(f" le nombre de livre est : {len(list_books)}")


#Lister les livres depuis 2014
def list_books2014():
    list_books2014 = list(publis.find({"type" : "Book", "year" : {"$gte" : 2014}}, {"_id" :0, "title": 1}))
    #pprint(list_books)
    print(f" le nombre de livre publié depuis 2014 est : {len(list_books2014)}")


#Lister les publications de l’auteur “Toru Ishida” 
def list_authors_toruishida():
    list_authors_toruishida = list(publis.find({"authors" : "Toru Ishida"}, {"_id" :0, "title": 1, "authors" : 1}))
    pprint(list_authors_toruishida )
    print(f" le nombre de publication de l'auteur Toru Ishida est : {len(list_authors_toruishida )}")



#Lister tous les auteurs distincts 
def list_authors():
    list_authors = list(publis.distinct("authors"))
    #pprint(list_authors)
    print(f" le nombre d'auteur est  : {len(list_authors)}")



#Trier les publications de “Toru Ishida” par titre de livre 

def list_publication_toruishida_book():
    list_publication_toruishida_book= list(publis.find({"authors" : "Toru Ishida"}, {"_id" :0, "title": 1, "authors" : 1}).sort("title"))
    pprint(list_publication_toruishida_book )
    print(f" le nombre de publication par titre de livre de l'auteur Toru Ishida est : {len(list_publication_toruishida_book )}")



#Compter le nombre de ses publications ;

def number_publication_toruishida_book():
    number_publication_toruishida_book= list(publis.find({"authors" : "Toru Ishida"}, {"title" : 1}))
    pprint(number_publication_toruishida_book )
    print(f" le nombre de publication par titre de livre de l'auteur Toru Ishida est : {len(number_publication_toruishida_book )}")



#Compter le nombre de publications depuis 2011 et par type 
def publi_since_2011():
    for publi in publis.distinct('type'):
        article = publis.find({'type': publi, 'year':{"$gte":2011}}).count()
        print(f"{article} {publi}")


#Compter le nombre de publications par auteur et trier le résultat par ordre croissant ;
def publi_auth_crois():
    authors = list(publis.aggregate([
        {'$unwind':'$authors'},
        {'$group':{'_id': '$authors', 'publi':{'$sum':1}}},
        {'$sort':{'publi':1}}]))
    pprint(authors)
    print(len(authors))

#publi_auth_crois()
#count_docs()
#list_books()
#list_books2014()
#list_authors_toruishida()
#list_authors()
#publi_since_2011
#list_publication_toruishida_book()
#number_publication_toruishida_book()




