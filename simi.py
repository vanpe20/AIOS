from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# 初始化 Sentence-BERT 模型
model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

# 两个待比较的句子
sentence1 = "You should extract the search key, directory from the sentence directly and separate them by commas"
sentence2 = "You should extract information from the sentence, you need output search key, directory from it and separate them by commas"

# 将句子转换为向量
embedding1 = model.encode(sentence1)
embedding2 = model.encode(sentence2)

# 计算余弦相似度
similarity = cosine_similarity([embedding1], [embedding2])

# 输出相似度分数
print(f"The cosine similarity between the sentences is: {similarity[0][0]:.4f}")
