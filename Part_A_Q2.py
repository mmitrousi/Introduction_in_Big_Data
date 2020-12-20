import sys
import gc
import os
import sqlalchemy
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import csv
import json
import math
import re
import datetime

# Create the database, use it and create tables as well
def create_database():
    print("Creating database...")
    cursor.execute("DROP DATABASE IF EXISTS books")
    cursor.execute("CREATE DATABASE books")
    cursor.execute("USE books")

    print("Creating tables...")
    cursor.execute("""CREATE TABLE books (
                    isbn VARCHAR(13) NOT NULL PRIMARY KEY,
                    book_title VARCHAR(100),
                    book_author VARCHAR(50))""")

    cursor.execute("""CREATE TABLE users (
                    user_id INT(10) NOT NULL PRIMARY KEY,
                    age INT)""")

    cursor.execute("""CREATE TABLE ratings (
                    user_id INT NOT NULL,
                    isbn VARCHAR(13) NOT NULL,
                    book_rating INT,
                    PRIMARY KEY (user_id, isbn),
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (isbn) REFERENCES books(isbn))""")

    print("Database successfully created !\n")

# Insert the data into the database
def insert_into_database(sql_engine, type):
    print("Inserting data into database...")

    if (type == "similarities"):
        csv_data = csv.reader('user-pairs-books.data.csv')
        for row in csv_data:
            cursor.execute('INSERT INTO testcsv(names, classes, mark )' 'VALUES("%s", "%s", "%s")', row)
    elif (type == "neighbors"):
        csv_data = csv.reader('neighbors-k-books.data.csv')
        for row in csv_data:
            cursor.execute('INSERT INTO testcsv(names, classes, mark )' 'VALUES("%s", "%s", "%s")', row)
    elif (type == "books"):
        books.to_sql(name = 'books', con = sql_engine, if_exists = 'replace', index = False)
    elif (type == "users"):
        users.to_sql(name = 'users', con = sql_engine, if_exists = 'replace', index = False)
    elif (type == "ratings"):
        ratings.to_sql(name = 'ratings', con = sql_engine, if_exists = 'replace', index = False)

    print("Data successfully inserted !")

# Exports to CSV file
def export_to_csv(data, name):
    print("Exporting to CSV file...")

    with open(name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data)

    print("File '{}' created !\n".format(name))

# Read CSV file, and store the data in a dataframe
def read_csv_file(csv_file):
    dataframe = pd.read_csv(csv_file, sep=";", error_bad_lines= False, low_memory=False, encoding="ISO-8859-1")
    return dataframe

# Generate ratings matrix from
def generate_ratings_matrix(ratings):
    book_ratings_threshold_perc = 0.05
    user_ratings_threshold = 10

    filter_users = ratings['User-ID'].value_counts()
    filter_users_list = filter_users[filter_users >= user_ratings_threshold].index.to_list()
    df_ratings_top = ratings[ratings['User-ID'].isin(filter_users_list)]

    book_ratings_threshold = len(df_ratings_top['ISBN'].unique()) * book_ratings_threshold_perc
    filter_books_list = df_ratings_top['ISBN'].value_counts().head(int(book_ratings_threshold)).index.to_list()
    ratings = df_ratings_top[df_ratings_top['ISBN'].isin(filter_books_list)]


    print("Creating pivot table...")
    ratings = ratings.groupby(['User-ID', 'ISBN'])['Book-Rating'].mean().astype("Sparse[int]")
    ratings = ratings.unstack(fill_value=0)
    print("Ratings Table: (users x books): {}".format(ratings.shape))

    ratings = ratings.to_numpy()
    print("Pivot table successfully created !\n")
    return ratings

# cosine similarity of users u and v
def csim(u, v):
    return (np.dot(u, v) / (np.linalg.norm(u) * np.linalg.norm(v)))

# Pearson similarity of users u and v
def psim(u, v):
    return csim(u - np.mean(u), v - np.mean(v))

# calc_similarities(r, sim)
# r: ratings table, users x items
# sim: similarity measure to be used, default csim
# calculates similarities between all user pairs
# returns table u x u, [i, j] element is similarity of ui and uj
def calc_similarities(r, sim=csim):
    print("Calculating similarities...")
    return np.corrcoef(r.T, rowvar=False)

# calc_neighbourhood(s, k)
# s: similarities matrix, u x u, symmetric
# k: size of neighbourhood
def calc_neighbourhood(s, k):
    print("Calculating neighbourhood...")
    return np.array([[x for x in np.argsort(s[i]) if x != i][len(s) - 1: len(s) - k - 2: -1] for i in range(len(s))])


# predict(userId, itemId, r, s, n)
# r: ratings table, user x item
# s: similarities matrix, user x user
# nb: neighbourhood table, user x k
def predict(userId, itemId, r, s, nb):
    rsum, ssum = 0.0, 0.0
    for n in nb[userId]:
        rsum += s[userId][n] * (r[n][itemId] - np.mean(r[n]))
        ssum += s[userId][n]
    return np.mean(r[userId]) + rsum / ssum


# mae(p, a) returns the mean average error between
# predictions p and actual ratings a
def mae(p, a):
    return sum(map(lambda x: abs(x[0] - x[1]), zip(p, a))) / len(p)


# rmse(p, a) returns the root mean square error between
# predictions p and actual ratings a
def rmse(p, a):
    return math.sqrt(sum(map(lambda x: (x[0] - x[1]) ** 2, zip(p, a))) / len(p))

# flatten(l) flattens a list of lists l
def flatten(l):
    return np.array([x for r in l for x in r])

def remove_user_outliers(users):
    users=users.loc[users['Age'] < 80.0]
    users=users.loc[users['Age'] > 15.0]

    return users

def remove_book_outliers(books):
    # Filter by valid ISBN Regex
    books = books[books.ISBN.str.contains('^\d{9}[\d|X]$', regex=True)]
    books = books[books["Year-Of-Publication"].str.contains(r"^\d+$")]

    # Cast to numeric
    books["Year-Of-Publication"] = books["Year-Of-Publication"].astype(int)

    # Keep only valid year of publication
    books = books[books["Year-Of-Publication"] <= 2020]
    books = books[books["Year-Of-Publication"] >= 1967]

    return books

def main():
    sql_engine = sqlalchemy.create_engine('mysql://root:root@localhost:3306/')

    books_csv = "BX-Books.csv"
    users_csv = "BX-Users.csv"
    ratings_csv = "BX-Book-Ratings.csv"

    books = remove_book_outliers(read_csv_file(books_csv))
    users = remove_user_outliers(read_csv_file(users_csv))
    ratings = read_csv_file(ratings_csv)

    start_time = datetime.datetime.now()

    r = generate_ratings_matrix(ratings)
    s = calc_similarities(r)
    export_to_csv(s, "user-pairs-books.data.csv")

    nb = calc_neighbourhood(s, 2)
    export_to_csv(nb, "neighbors-k-books.data.csv")

    print("Computing recommendations...")
    pr = [[predict(u, i, r, s, nb) for i in range(len(r[u]))] for u in range(len(r))]
    print("Computations completed !\n")

    print('Mean Average Error: {:.4f}'.format(mae(flatten(r), flatten(pr))))
    print('Root Mean Square Error: {:.4f}\n'.format(rmse(flatten(r), flatten(pr))))

    end_time = datetime.datetime.now()

    print('Computations Started: {}'.format(start_time))
    print('Computations Finished: {}'.format(end_time))

main()