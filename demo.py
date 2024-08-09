import os
import urllib.parse

# 文件路径
file_path = "/Users/manchester/Documents/rag/rag_source/rag_paper/AIOS.pdf"

# 生成访问链接
file_url = urllib.parse.urljoin('file:', urllib.parse.quote(os.path.abspath(file_path)))

print(file_url)