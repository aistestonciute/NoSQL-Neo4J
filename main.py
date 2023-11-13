import pymongo
from pprint import pprint
import Book  # Importing Book module, assuming it contains the Book class
import Review  # Importing Review module, assuming it contains the Review class
from pymongo import MongoClient

try:
    # Attempting to establish a connection to the MongoDB server
    client = MongoClient('localhost', 27017)
    print("\nSuccessfully connected!")
except:
    # If connection fails, print an error message
    print("\nConnection failed.")

# Accessing the 'main' database
db = client.main
books = db.book  # Assuming there's a collection named 'book' in the 'main' database
reviews = db.review  # Assuming there's a collection named 'review' in the 'main' database

books.drop()
reviews.drop()

# Function to insert data into the 'book' and 'review' collections
def insertData():
    books.insert_many([Book.b1, Book.b2, Book.b3, Book.b4, Book.b5, Book.b6])
    reviews.insert_many([Review.r1, Review.r2, Review.r3, Review.r4, Review.r5, Review.r6, Review.r7, Review.r8, Review.r9, Review.r10])

# Function to display the names of all collections in the 'main' database
def showCollections():
    print("\nPrinting information about collections: ")
    print(db.list_collection_names())

def showBooks():
    print ("\nPrinting information about books:")
    cursor = books.find({})
    for doc in cursor:
        pprint(doc, sort_dicts=False)


def showReviews():
    print("\nPrinting reviews:")
    cursor = reviews.find({})
    for doc in cursor:
        pprint(doc, sort_dicts=False)


def checkBook(title):
    if books.count_documents({"title": title}) > 0:
        return
    else:
        # If the book doesn't exist, raise a ValueError
        raise ValueError("The book does not exist")

# Function to display information about the author of a book
def authorInformation(title):
    checkBook(title)
    query = {"title": title}
    doc = books.find(query, {"author"})
    print("\nPrinting information about the author of " + title + ":")
    for i in doc:
        pprint(i, sort_dicts=False)

# Function to display information about the count of reviews per user
def reviewCount():
    print("\nInformation about users' review count: \n")
   
    pipeline = [{"$group": {"_id": "$user", "countReviews": {"$count": {}}}}]
    rows = list(reviews.aggregate(pipeline))
    print(rows)

# Function to display information about the count of reviews per user using MapReduce
def reviewCount_MapReduce():
    print("\nInformation about users' review count using map reduce: \n")
    mapF = "function() { emit(this.user, 1) }"
    reduceF = "function(k, v) { return Array.sum(v)}"
    result = db.command(
        'mapReduce',
        'review',
        map=mapF,
        reduce=reduceF,

        out={'inline': 1}
    )
    print(result)

# Function to display review count information and call the MapReduce function
def reviewCountPrint():
    reviewCount()
    reviewCount_MapReduce()

# Main function to execute the program
def main():
    insertData()
    while True:
        print("""
        Select the action:
        1 - Show collections
        2 - Show books' data
        3 - Show reviews' data
        4 - Show information about book's author
        5 - Show number of reviews written by user
        6 - Exit
        """)

        action = input('Enter the number of the action: ')

        if(action == '1'):
            showCollections()

        elif (action == '2'):
            showBooks()

        elif(action == '3'):
            showReviews()

        elif(action == '4'):
            title = input("Enter the title of the book: ")
            authorInformation(title)

        elif(action == '5'):
            reviewCountPrint()

        elif(action == '6'):
            print("The program is closing...")
            break

        else: raise ValueError("The action number is incorrect")

if __name__ == '__main__':
    main()
