
from .base import BaseStorage
from pathlib import PurePosixPath
import json
import shutil
import os
from llama_index.core import PromptTemplate
import chromadb
from chromadb.api.types import Metadata
import numpy as np
from llama_index.core import VectorStoreIndex, SimpleDirectoryReader
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core import StorageContext,Document
from llama_index.core.retrievers import VectorIndexRetriever
import uuid
import redis
from elasticsearch import Elasticsearch
import logging
# from db_storage import DBStorage
from .db_storage import DBStorage
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT
from whoosh.qparser import QueryParser
import shutil


class Data_Op(DBStorage):
    def __init__(self,retri_dic):
        pool = redis.ConnectionPool(host='localhost', port=6379, decode_responses=True)
        self.redis_client = redis.Redis(connection_pool=pool)
        self.embed_model =  HuggingFaceEmbedding(model_name="BAAI/bge-base-en-v1.5")
        self.retri_dic = retri_dic
        super().__init__(self.redis_client, self.embed_model,self.retri_dic)



    def create(self,db_path,db_name,doc):
        # create chroma database by single doc or file
        if not os.path.exists(doc):
            super().create_or_get_collection(db_path,db_name,doc)
        else:
            if os.listdir(doc):
                for root, dirs, filenames in os.walk(doc):
                        for filename in filenames:
                            print(filename)
                            docu = os.path.join(root, filename)
                            super().create_or_get_collection(db_path,db_name,filename,docu)

        # return super().create_or_get_collection(db_path,db_name)
    def insert(self,db_path,db_name,doc,metaname):
            
        return super().add_in_db(db_path,db_name,doc,metaname)

    def retrieve(self,db_path,db_name,query,type='meaning',loc=None):

        if type == 'full_text':
            return super().full_text_retrieve(db_path,db_name,query,loc=loc)
        elif type == 'meaning':
            return super().sym_retrieve(db_path,db_name,query,loc=loc)
        else:
            raise ValueError("retrieve type error")

    
    def update(self,db_path,db_name,doc,obj=None):

        return super().change_db(db_path,db_name,doc,obj)
    
    def delete(self,db_path,db_name,metaname):

        return super().del_db(db_path,db_name,metaname)
    

    

    def group_by(self, db_path, query, new_name, top_k=None):
        #group by with key word and build a new dababase
        ans = self.from_some_key(db_path,query,top_k=top_k)
        self.create(db_path, new_name, doc=ans)
        return db_path, new_name
    

    def from_some_key_full(self, db_path, query, db_name = None, top_k=None, con = None):
        logging.basicConfig(filename='results.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        # return the content with same keywords 
        ans = []
        rk = {}
        name_ans = []
        if db_name is not None:
            search_path = os.path.join(db_path,db_name)
            chroma_client = chromadb.PersistentClient(path=search_path)
            collections = chroma_client.list_collections()
            for collection in collections:
                # chroma_collection = chroma_client.get_collection(name)
                doc = collection.get()['documents']
                # print(collection.name)
                # print(doc)
                # logging.info(doc)
                
                doc = " ".join(doc)    
                schema = Schema(content=TEXT(stored=True))
                index_dir = collection.name
                if not os.path.exists(index_dir):
                    os.mkdir(index_dir)
                index = create_in(index_dir, schema)

                writer = index.writer()
                writer.add_document(content=doc)
                writer.commit()
                with index.searcher() as searcher:
                    if isinstance(query, list):
                        que = None
                        for condition in query:
                            tmp_que = QueryParser("content", index.schema).parse(condition)
                            if con == 'and':
                                que = que & tmp_que
                            elif con == 'or':
                                que = que | tmp_que
                    else:
                        que = QueryParser("content", index.schema).parse(query)
                    results = searcher.search(que)
                    if len(results) > 0:
                        ans.append(doc)
                        name_ans.append(collection.name)
                shutil.rmtree(index_dir)
        else:
            for root,dirs,files in os.walk(db_path):
                for dir in dirs:
                    search_path = os.path.join(db_path,dir)
                    chroma_client = chromadb.PersistentClient(path=search_path)
                    collections = chroma_client.list_collections()
                    for collection in collections:
                        # chroma_collection = chroma_client.get_collection(name)
                        doc = collection.get()['documents']                        
                        doc = " ".join(doc)    
                        schema = Schema(content=TEXT(stored=True))
                        index_dir = collection.name
                        if not os.path.exists(index_dir):
                            os.mkdir(index_dir)
                        index = create_in(index_dir, schema)

                        writer = index.writer()
                        writer.add_document(content=doc)
                        writer.commit()
                        with index.searcher() as searcher:
                            if isinstance(query, list):
                                que = None
                                for condition in query:
                                    tmp_que = QueryParser("content", index.schema).parse(condition)
                                    if con == 'and':
                                        que = que & tmp_que
                                    elif con == 'or':
                                        que = que | tmp_que
                            else:
                                que = QueryParser("content", index.schema).parse(query)
                            results = searcher.search(que)
                            if len(results) > 0:
                                ans.append(doc)
                                name_ans.append(collection.name)
                        shutil.rmtree(index_dir)
                        
            #     es = Elasticsearch(['http://localhost:9200'])
            #     file = {
            #     "text": str(doc),
            #     "title": collection.name,
            #     }
            #     # parts = collection.name.split('.')[0]
            #     if not es.indices.exists(index=collection.name.lower()):
            #         es.index(index=collection.name.lower(), body = file)
            #     res = es.search(index=collection.name.lower(), body={"query": {"match_phrase": {"content": query}}})

            #     if res['hits']['total']['value'] > 0:
            #         if top_k is None:
            #             ans.append(doc)
            #             name_ans.append(collection.name)
            #         else:
            #             rk[collection.name] = res['hits']['hits'][0]['_score']
            #     else:
            #         continue
            # if top_k:
            #     sorted_dict_desc = dict(sorted(rk.items(), key=lambda item: item[1], reverse=True))
            # # print(sorted_dict_desc)
            #     keys_iterator = iter(sorted_dict_desc)
            #     for i in range(top_k):
            #         key = next(keys_iterator)
            #         collection = chroma_client.get_collection(key)
            #         ans.append(collection.get()['documents'])
            #         name_ans.append(key)

                    # if top_k:
                    #     sorted_dict_desc = dict(sorted(rk.items(), key=lambda item: item[1], reverse=True))
                    #     keys_iterator = iter(sorted_dict_desc)
                    #     for i in range(top_k):
                    #         key = next(keys_iterator)
                    #         collection = chroma_client.get_collection(key)
                    #         ans.append(collection.get()['documents'])
                    #         name_ans.append(key)
        return ans,name_ans
    def from_some_key_sy(self,db_path,query,top_k,db_name = None):
        logging.basicConfig(filename='results.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        # return the content with same keywords 
        ans = []
        rk = {}
        name_ans = []
        if db_name is not None:
            search_path = os.path.join(db_path,db_name)
            chroma_client = chromadb.PersistentClient(path=search_path)
            collections = chroma_client.list_collections()
            for collection in collections:
                # print(name)
                # chroma_collection = chroma_client.get_collection(name)
                vector_store = ChromaVectorStore(chroma_collection=collection)
                index = VectorStoreIndex.from_vector_store(vector_store,embed_model=self.embed_model)
                retriever = VectorIndexRetriever(index=index, similarity_top_k=2)
                result = retriever.retrieve(query)
                rk[collection.name] = result[0].score

        else:
            for root,dirs,files in os.walk(db_path):
                for dir in dirs:
                    search_path = os.path.join(db_path,dir)
                    chroma_client = chromadb.PersistentClient(path=search_path)
                    name = chroma_client.list_collections()
                    for name in collections:
                        chroma_collection = chroma_client.get_collection(name)
                        vector_store = ChromaVectorStore(chroma_collection=collection)
                        index = VectorStoreIndex.from_vector_store(vector_store,embed_model=self.embed_model)
                        retriever = VectorIndexRetriever(index=index, similarity_top_k=2)
                        result = retriever.retrieve(query)
                        rk[collection.name] = result[0].score

        sorted_dict_desc = dict(sorted(rk.items(), key=lambda item: item[1], reverse=True))
        keys_iterator = iter(sorted_dict_desc)
        for i in range(top_k):
            key = next(keys_iterator)
            ans.append(chroma_collection.get(key)['documents'])
            name_ans.append(collection.name)
            
        return ans,name_ans
    
    def join(self,db_path, db_name1, metaname1, metaname2, db_name2 = None):
        # add 2 to 1 
        if db_name2 is None:
            obj_path = os.path.join(db_path,db_name1)
            chroma_client = chromadb.PersistentClient(path=obj_path)
            chroma_collection2 = chroma_client.get_collection(metaname2)

            doc = chroma_collection2.get()['documents']

            super().add_in_db(db_path, db_name1, doc, metaname1)
            super().del_db(db_path, db_name1, metaname2)
        else:
            obj_path1 = os.path.join(db_path,db_name1)
            obj_path2 = os.path.join(db_path,db_name2)
            chroma_client1 = chromadb.PersistentClient(path=obj_path1)
            chroma_client2 = chromadb.PersistentClient(path=obj_path2)
            chroma_collection2 = chroma_client2.get_collection(metaname2)
            doc = chroma_collection2.get()['documents']
            super().add_in_db(db_path, db_name1, doc, metaname1)
            super().del_db(db_path, db_name2, metaname2)

    def group_retrieve(self, db_path, query, db_name=None):
        # retrieve query in content related to keywords, like where in sql

        doc, name = self.from_some_key_full(db_path,query,db_name=db_name)
        documents = [Document(id=str(uuid.uuid4()), text=doc_content) for doc_content in doc]
        index = VectorStoreIndex.from_documents(documents)
        retriever = VectorIndexRetriever(index,similarity_top_k=2)
        result = retriever.retrieve(query)
        ans = result[0].get_content()
        ans = ''.join(ans)
        return ans, name

    def get_collection(self,db_path,db_name):
        collection = self.create_or_get_collection(db_path,db_name)
        print(collection.database)
        # res = collection.get(ids=['188a8336-eab0-455b-b78e-28e011e34b19'])
        # print(res)
        # print(collection)
        
    

        
        




                



