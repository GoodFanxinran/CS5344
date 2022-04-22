import os
import pyspark
import re
import nltk
nltk.download('wordnet')
import numpy as np
np.seterr(invalid='ignore')
from nltk.stem.lancaster import LancasterStemmer
from textblob import Word

conf = pyspark.SparkConf()
sc = pyspark.SparkContext(conf=conf)

# datafiles_folder = sys.argv[1]
# stopwords_file_path = sys.argv[2]
# query_file_path = sys.argv[3]
# out_file_path = sys.argv[4]

datafiles_folder = 'datafiles'
stopwords_file_path = 'stopwords.txt'
out_file_path = 'outfile/output.txt'
query_file_path = 'query.txt'
out_file_path2 = 'outfile/output2.txt'
out_file_path3 = 'outfile/result.txt'
out_file_path4 = 'outfile/result2.txt'


#Obtain the stopwords
stopwords = [sw.strip("\n") for sw in open(stopwords_file_path, "r").readlines()]

# ============================================================================
# Step0 Preprocessing
# 1. Retain non-empty files
# 2. Remove URLs
# 3. Lowercase
# 4. Remove stopwords, punctuations, numbers
# 5. Lemmatization
# 6. Combine sentence using ' '
# =============================================================================

all_files = (os.path.join(datafiles_folder, str(x)+'.txt') for x in range(151))
directory = ",".join(all_files)

file_rdd = sc.textFile(directory)

# def lemmatization(word):
#     lemmatizer = WordNetLemmatizer()
#     w = lemmatizer.lemmatize(word)
#     return w

def clean_data(data):
    vl = []
    re.sub('http://\S+|https://\S+|www\S+', '', data)
    v = re.split(r'\s|(?<!\d)[,.]|[,.](?!\d)|[-]', data)
    for w in v:
        lower_w = w.lower()
        w2 = re.sub('[^A-Za-z0-9]+', '', lower_w)
        # vl.append(((lower_w, tail), 1))
        vl.append(w2)
    return vl

# #.map(lambda x: lemmatization(x))
data = file_rdd.flatMap(clean_data).filter(lambda x: x != "" and x not in stopwords)



# #============================================================================
# # Step 1
# # Compute term frequency (TF) of every word in a document.
# # Compute document frequency (DF) of every word.
# # =============================================================================
def tf_computation(document):
    word_count = dict()
    for item in document:
        word = item[0]
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    for (word, count) in [(k, word_count[k]) for k in word_count]:
        yield (word, count)


tf = (
    data.map(lambda x: (x, 1))
    .mapPartitions(lambda x: tf_computation(x))
)


def df_computation(document):
    document_count = dict()
    for word in document:
        if word not in document_count.keys():
            document_count[word] = 1
            yield (word, 1)

df = dict(
    data.mapPartitions(lambda x: df_computation(x)).reduceByKey(lambda a,b: a+b).collect()
    )


# # ============================================================================
# # Step 3
# # 1. Compute TF-IDF of every word w.r.t a document.
# # 2. Normalize TF-IDF value for every word
# # =============================================================================
N = data.getNumPartitions()

def tfidf_computation(document,df,N):
    document = list(document)
    tfidf_dict = dict.fromkeys(df.keys(), 0)
    for tf_element in document:
        word, tf = tf_element
        tfidf_dict[word] = (1+(np.log10(tf))) * np.log10(N / df[word])
    for word, tfidf in tfidf_dict.items():
        yield (word, tfidf)

tfidf = (
    tf.mapPartitions(lambda x: tfidf_computation(x,df,N))
)


def tfidf_normalized(document):
    document = list(document)
    S=0
    for word, tfidf in document:
        S  += tfidf**2

    if(S!=0):
        for tfidf_element in document:
            word, tfidf = tfidf_element
            yield (word, tfidf / np.sqrt(S))
    else:
        for tfidf_element in document:
            word, tfidf = tfidf_element
            yield (word, 1000)

norm_tfidf = dict(
    tfidf.mapPartitions(lambda x: tfidf_normalized(x))
    .groupByKey()
    .mapValues(list)
    .collect()
)



word_list = list(norm_tfidf.keys())
norm_tfidf_matrix = np.array(list(norm_tfidf.values()))


# #============================================================================
# # Step 4
# # 1. Compute the relevance of each document w.r.t a query
# #=============================================================================
query= sc.textFile(query_file_path)

query_list = (query
            .flatMap(lambda l: re.split(r'\s|(?<!\d)[,.]|[,.](?!\d)', l))
            .filter(lambda l1: l1 != None)
            .collect()
              )

document_word_list = dict.fromkeys(word_list, 0)

for query_w in query_list:
    if query_w in document_word_list:
        document_word_list[query_w] = 1
query_vector = np.array(list(document_word_list.values()))


def cosine_calculation(query, norm_tfidf_matrix):
    return np.dot(query, norm_tfidf_matrix) / (np.linalg.norm(query) * np.linalg.norm(norm_tfidf_matrix))


sim = []
for n in range(N):
    V = [i[n] for i in norm_tfidf_matrix]
    sim1 = cosine_calculation(query_vector, V)
    sim.append(sim1)


# # ==========================================================================
# # Step 5
# # Sort and get top-10 documents
# # =============================================================================
relevances = sorted([(doc, sim) for doc, sim in enumerate(sim)], key=lambda x: x[1], reverse=True)


k = 10
top10_doc = []
for (doc, sim) in relevances[:k]:
    top10_doc.append(doc)

with open(out_file_path, 'w+') as f:
    for (doc_id, relevance) in relevances[:k]:
        f.write(f"{doc_id} {relevance}\n")

# ==========================================================================================
# Step 6
# For each of the top-10 document, compute the relevance of each sentence w.r.t the query.
# A sentence is delimited by a full-stop
# ==========================================================================================

def split_data(data):
    vl = []
    v = re.split(r'[.!?]', data)
    for w in v:
        vl.append(w)
    return vl

for x in top10_doc:
    directory = os.path.join(datafiles_folder, str(x)+'.txt')
    file = sc.textFile(directory)
    doc_con = file.flatMap(lambda x: split_data(x))

    sen_list1 = doc_con.collect()

    sen_list = []
    for y in sen_list1:
        if(re.match(r'[^A-Za-z0-9]+',y)):
            pass
        else:
            sen_list.append(y)

    len_sen = len(sen_list)
    N2=len_sen

    sen_rdd = sc.parallelize(sen_list,len_sen)
    data2 = sen_rdd.flatMap(clean_data).filter(lambda x: x != "" and x not in stopwords)
#     # .map(lambda x: lemmatization(x))


    tf2 = (
        data2.map(lambda x: (x, 1))
            .mapPartitions(lambda x: tf_computation(x))
    )


    df2 = dict(
        data2.mapPartitions(lambda x: df_computation(x)).reduceByKey(lambda a, b: a + b).collect()
    )


    tfidf2 = (
        tf2.mapPartitions(lambda x: tfidf_computation(x, df2, N2))
    )


    norm_tfidf2 = dict(
        tfidf2.mapPartitions(lambda x: tfidf_normalized(x))
            .groupByKey()
            .mapValues(list)
            .collect()
    )

    word_list2 = list(norm_tfidf2.keys())
    norm_tfidf_matrix2 = np.array(list(norm_tfidf2.values()))
    document_word_list2 = dict.fromkeys(word_list2, 0)
    for query_w in query_list:
        if query_w in document_word_list2:
            document_word_list2[query_w] = 1
    query_vector2 = np.array(list(document_word_list2.values()))
    sim2 = []
    for n in range(N2):
        V = [i[n] for i in norm_tfidf_matrix2]
        sim1 = cosine_calculation(query_vector2, V)
        sim2.append(sim1)

    relevances2 = sorted([(doc, sim) for doc, sim in enumerate(sim2)], key=lambda x: x[1], reverse=True)
    k = 1
    with open(out_file_path2, 'a+') as f:
        for (doc_id, relevance) in relevances2[:k]:
            f.write(f"({x},{sen_list[doc_id]},{relevance})\n")

sc.stop()
