from dask import dataframe as dd
import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

""" You can download dataset from
https://dumps.wikimedia.org/other/clickstream/2018-12/clickstream-enwiki-2018-12.tsv.gz
or by:
wget https://dumps.wikimedia.org/other/clickstream/2018-12/clickstream-enwiki-2018-12.tsv.gz -O ./data/clickstream_data.tsv.gz
gunzip ./data/clickstream_data.tsv.gz
"""


df = pd.read_csv("data/clickstream_data.tsv",
                 na_values=0,
                 delimiter="\t",
                 names=["coming_from", "article", "referrer_type", "n"],
                 dtype={
                     "referrer_type": "category",
                     "n": "uint32"}
                 )

df = df.iloc[:100000]

""" :) info :) """
# print(df_all.info())
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 29843928 entries, 0 to 29843927
# Data columns (total 4 columns):
# coming_from      object
# article          object
# referrer_type    category
# n                uint32
# dtypes: category(1), object(2), uint32(1)
# memory usage: 597.7+ MB
# None


top_links = df.loc[
    df['referrer_type'].isin(['link']),
    ['coming_from', 'article', 'n']
] \
    .groupby(['coming_from', 'article']) \
    .sum() \
    .sort_values(by='n', ascending=False)

print(top_links)

dfd = dd.read_csv(
    'data\clickstream_data.tsv',
    delimiter='\t',
    names=['coming_from', 'article', 'referrer_type', 'n'],
    dtype={
        'referrer_type': 'category',
        'n': 'uint32'},
    blocksize=64000000  # = 64 Mb chunks
)

top_links_grouped_dask = dfd.loc[
    dfd['referrer_type'].isin(['link']),
    ['coming_from', 'article', 'n']] \
    .groupby(['coming_from', 'article'])

store = pd.HDFStore('./data/clickstream_store.h5')

top_links_dask = top_links_grouped_dask.sum().nlargest(20, 'n')


store.put('top_links_dask',
          top_links_dask.compute(),
          format='table',
          data_columns=True)

""" Q1: Which links do people click on most often in a given article?"""
df_most_popular = pd.read_hdf('./data/clickstream_store.h5')

print(df_most_popular)

""" Q2: What are the most popular articles users access from all the external search engines? """

external_searches = df.loc[
 (df['referrer_type'].isin(['external'])) &
 (df['coming_from'].isin(['other-search'])),
 ['article', 'n']
]

most_popular_articles = external_searches.sort_values(
    by='n', ascending=False).head(40)

print(most_popular_articles)

""" + DASK """

external_searches_dask = dfd.loc[
    (dfd['referrer_type'].isin(['external'])) &
    (dfd['coming_from'].isin(['other-search'])),
    ['article', 'n']
]

external_searches_dask = external_searches_dask.nlargest(
    40, 'n').compute()


sns.barplot(data=external_searches_dask, y='article', x='n')
plt.gca().set_ylabel('')

""" Q3: What percentage of visitors to a given article page have clicked on a link to get there? """


def visitors_clicked_link_pandas(dataframe, article):
    df_article = dataframe.loc[dataframe['article'].isin([article])]
    a = df_article['n'].sum()
    l = df_article.loc[
        df_article['referrer_type'].isin(['link']),
        'n'].sum()
    return round((l*100)/a, 2)


visitors_clicked_link_pandas(df, 'Jehangir_Wadia')
# 81.1


""" Pandas + Dask """


def visitors_clicked_link_dask(dataframe, article):
    df_article = dataframe.loc[dataframe['article'].isin([article])]
    a = df_article['n'].sum().compute()
    l = df_article.loc[
        df_article['referrer_type'].isin(['link']),
        'n'].sum().compute()
    return round((l*100)/a, 2)


visitors_clicked_link_dask(dfd, 'Jehangir_Wadia')
# 81.1

""" Q4: What is the most common source of visits for each article? """

summed_articles = df.groupby(['article', 'coming_from']).sum()

max_n_filter = summed_articles.reset_index()\
    .groupby('article')\
    .idxmax()


summed_articles.iloc[max_n_filter['n']].head(4)

# Out[21]:
#                                            n
# article             coming_from
# !Kung_languages     other-empty         1464
# $1,000_genome       other-search         801
# 'S_Out              Bottom_(TV_series)    92
# (120132)_2003_FY128 other-empty           17


summed_articles.iloc[max_n_filter['n']]\
    .sort_values(by='n', ascending=False)\
    .head(10)


# Out[22]:
#                                                n
# article                  coming_from
# Zero_(2018_film)         other-search    1923035
# College_Football_Playoff other-search     298487
# Attack_on_Pearl_Harbor   other-search     292416
# O._J._Simpson            other-search     168402
# Outlaw_King              other-search     151554
# MS_Dhoni                 other-search     143811
# Saint_Petersburg         other-external    99437
# Seth_Curry               other-search      97616
# Mark_Harmon              other-search      88395
# Joel_Embiid              other-search      84820

#
# summed_articles = dfd.groupby(['article', 'coming_from'])\
#     .sum()\
#     .reset_index()\
#     .compute()
#
# dfd.groupby(['article', 'coming_from'])\
#     .sum()\
#     .reset_index()\
#     .to_parquet('./summed_articles.parquet', engine='pyarrow')

