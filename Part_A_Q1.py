from mpl_toolkits.mplot3d import Axes3D
from sklearn.preprocessing import StandardScaler
import seaborn as sns
import matplotlib.pyplot as plt # plotting
import numpy as np # linear algebra
import os # accessing directory structure
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Read and parse lines
books_col_list = ["ISBN", "Book-Title","Book-Author","Year-Of-Publication","Publisher"]
books = pd.read_csv("/Users/user/Desktop/BX-Books.csv",usecols=books_col_list, header=0, sep=';', encoding='latin-1',low_memory=False)
users = pd.read_csv("/Users/user/Desktop/BX-Users.csv", sep=";", encoding='latin-1')
ratings = pd.read_csv("/Users/user/Desktop/BX-Book-Ratings.csv", sep=";", encoding='latin-1')
books.describe()
users.describe()
ratings.describe()
books = pd.read_csv("/Users/user/Desktop/BX-Books.csv",usecols=books_col_list, header=0, sep=';', encoding='latin-1',low_memory=False)

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
    
books = remove_book_outliers(books)
books.hist(column="Year-Of-Publication")
books.describe()
#Cleaning Users Data by droping irregular values and secifies the age ranges we need
def remove_user_outliers(users):
    users=users.loc[users['Age'] < 80.0]
    users=users.loc[users['Age'] > 15.0]
    
    return users
    
books = remove_user_outliers(users)
books.hist(column="Age")
books.describe()
#keeping unique ID values
users.drop_duplicates(subset=("User-ID"))
# Merge ratings with books
books_ratings = pd.merge(ratings, books, how="inner", left_on='ISBN', right_on='ISBN')
books_read_count = books_ratings.groupby("ISBN").count()
books_read_count.hist(column="User-ID")
print("Before removing books with outlying read counts: ", books_read_count['User-ID'].count())

# Remove outlying user_id counts by z_scores
std = books_read_count['User-ID'].std()
mean = books_read_count['User-ID'].mean()
books_read_count['User-ID-Z-Score'] = (books_read_count['User-ID'] - mean) / std
books_read_count_outliers = books_read_count[books_read_count['User-ID-Z-Score'].abs() > 3]
books_read_count = books_read_count[books_read_count['User-ID-Z-Score'].abs() < 3]
print("After removing books with outlying read counts:", books_read_count['User-ID'].count())
print("Removed outliers:", books_read_count_outliers['User-ID'].count())

books_read_count.hist(column="User-ID")

#books_read_count["User-ID"].describe()
#books_read_count['User-ID-Z-Score'].describe()
authors_c = authors_m.groupby("Book-Author").count()
age = authors_m.groupby("Book-Author").count()
del authors_c["User-ID"]
del authors_c["ISBN"]
print("The top 20 Authors sorted by popularity based on book ratings:")
authors_c.sort_values(["Book-Rating"],ascending=False).head(20)

btitles_c = btitles_m.groupby("Book-Title").count()
del btitles_c["User-ID"]
del btitles_c["ISBN"]
print("The top 20 Books sorted by popularity based on book ratings:")
btitles_c.sort_values(["Book-Rating"],ascending=False).head(20)

merge_users_with_ratings= pd.merge(ratings, users, how="inner", left_on='User-ID', right_on='User-ID')
ages_m = merge_users_with_ratings[["User-ID","Age", "ISBN", "Book-Rating"]].copy()
ages_c=ages_m.groupby("Age").count()
del ages_c["User-ID"]
del ages_c["ISBN"]
print("The top 20 Ages sorted by popularity based on book ratings:")
ages_c.sort_values(["Book-Rating"],ascending=False).head(20)

# Remove Outliers
def remove_outliers(dataframe):

    # Calculating upper and lower limits
    upperlimit=dataframe["Book-Rating"].mean()+3*dataframe['Book-Rating'].std()
    lowerlimit=dataframe["Book-Rating"].mean()-3*dataframe['Book-Rating'].std()
    
    # Remove outliers
    outliers = dataframe[(dataframe['Book-Rating'] > upper_limit) | (dataframe["Book-Rating"]<lower_limit) ].index
    dataframe = dataframe.drop(outliers)

    return dataframe
