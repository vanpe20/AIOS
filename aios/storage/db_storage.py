# TODO: Not implemented
# Storing to databases has not been implemented yet
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
import uuid
from elasticsearch import Elasticsearch
import logging
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT
from whoosh.qparser import QueryParser

class DBStorage:
    # def __init__(self, agent_name, ragdata_path,db_path):
    def __init__(self, redis_client,embed_model,retri_dic):
        # self.agent_name = agent_name
        # self.client = chromadb.Client()
        # # self.collection = self._create_or_get_collection()
        # self.data_path = ragdata_path
        # self.db_path = db_path
        # self.llm = llm
        self.embed_model = embed_model
        self.redis_client = redis_client
        self.retri_dic = retri_dic

    def create_or_get_collection(self,db_path,db_name,metaname=None,doc=None):

        path = os.path.join(db_path,db_name)
        if doc is None:
            chroma_client = chromadb.PersistentClient(path=path)
            chroma_collection = chroma_client.get_collection(metaname)
            # vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            # storage_context = StorageContext.from_defaults(vector_store=vector_store)
            # index = VectorStoreIndex(storage_context=storage_context, embed_model=self.embed_model)
            # return chroma_collection
        
        else:
            chroma_client = chromadb.PersistentClient(path=path)
            chroma_collection = chroma_client.get_or_create_collection(metaname)
            if os.path.exists(doc):
                documents = SimpleDirectoryReader(input_files=[doc]).load_data()
                merged_content = " ".join(doc.text for doc in documents)
                document = merged_content
            else:
                doc_id = str(uuid.uuid4())
                document = [doc]
                document = Document(id=doc_id, text=document)
            # embedding = self.embed_model._embed(document)
            # chroma_collection.add(ids=[id], embeddings=embedding)
            # print(chroma_collection.get())
            # print(ssd)
            vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            storage_context = StorageContext.from_defaults(vector_store=vector_store)
            # index = VectorStoreIndex.from_vector_store(vector_store, embed_model=self.embed_model)
            index = VectorStoreIndex.from_documents(documents,storage_context=storage_context, embed_model=self.embed_model)
        path = os.path.join(path,metaname)
        index.storage_context.persist(persist_dir=path)
        
        # return chroma_collection
            
    def add_in_db(self, db_path, db_name, doc, metaname):
        add_path = os.path.join(db_path,db_name,metaname)
        if not os.path.exists(add_path):
            index = self.create_or_get_collection(db_path,db_name,metaname,doc)
        else:
            chroma_client = chromadb.PersistentClient(path=add_path)
            chroma_collection = chroma_client.get_or_create_collection(metaname)
            id = str(uuid.uuid4())
            if os.path.exists(doc):
                documents = SimpleDirectoryReader(input_files=[doc]).load_data()
                merged_content = " ".join(doc.text for doc in documents)
                document = merged_content
            else:
                document = [doc]
                
            if self.redis_client.exists(metaname):
                self.redis_client.append(metaname,documents)
            
            embedding = self.embed_model._embed(document)
            chroma_collection.add(ids=[id], embeddings=embedding)
            vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            index = VectorStoreIndex.from_vector_store(vector_store, embed_model=self.embed_model)
        index.storage_context.persist(persist_dir=add_path)
 

    def del_db(self,db_path,db_name,metaname = None,text = None):
        if metaname is None and text is None:
            raise ValueError('Must have one of metaname and text as arguments')
        del_path = os.path.join(db_path,db_name)
        if not os.path.exists(del_path):
            raise FileNotFoundError('delete path is not exist')
        else:    
            if metaname:
                del_path = os.path.join(del_path,metaname)
                chroma_client = chromadb.PersistentClient(path=del_path)
                chroma_collection = chroma_client.get_collection(metaname)
                chroma_collection = chroma_collection.delete()
            else:
                chroma_client = chromadb.PersistentClient(path=del_path)
                collections = chroma_client.list_collections()
                for collection in collections:
                        req = {"$contains": text}
                        collection = collection.get(where_document=req)
                        ids = collection['ids']
                        if ids is None:
                            continue
                        else:
                            chroma_collection = collection.delete(ids=ids)
            vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            index = VectorStoreIndex.from_vector_store(vector_store,embed_model=self.embed_model)
            index.storage_context.persist(persist_dir=del_path)

        if metaname:
            if self.redis_client.exists(metaname):
                self.redis_client.delete(metaname)

    
    def change_db(self,db_path,db_name,doc,metaname):
        change_path = os.path.join(db_path,db_name,metaname)
        if not os.path.exists(change_path):
            raise FileNotFoundError('change path is not exist')
        else:
            chroma_client = chromadb.PersistentClient(path=change_path)
            chroma_collection = chroma_client.get_collection(metaname)
            ids = chroma_collection.id
            if os.path.exists(doc):
                # documents = SimpleDirectoryReader(doc).load_data()
                documents = SimpleDirectoryReader(input_files=[doc]).load_data()
                merged_content = " ".join(doc.text for doc in documents)
                document = merged_content
            else:
                # document = Document(id=id, content=doc)
                document = [doc]
            embedding = self.embed_model._embed(document)
            chroma_collection.update(ids = ids, embeddings=embedding)
            vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
            index = VectorStoreIndex.from_vector_store(vector_store, embed_model=self.embed_model)
            index.storage_context.persist(persist_dir=change_path)
        
        if self.redis_client.exists(metaname):
            self.redis_client.set(metaname, documents)
        return chroma_collection
    

    def full_text_retrieve(self,db_path,db_name,query,metaname = None):
        search_path = os.path.join(db_path,db_name)
        chroma_client = chromadb.PersistentClient(path=search_path)
        chroma_collection = chroma_client.get_collection(metaname)
        data = self.redis_client.lrange(loc, 0, -1)
        if data:
            self.retri_dic[metaname] +=1
            doc = data
        else:
            if metaname is None:
                doc = chroma_collection.get()['documents']
            else:
                # req = {"$contains": loc}
                collection = chroma_collection.get()
                doc = collection['documents']
                doc = " ".join(collection['documents'])
      


        es = Elasticsearch(['http://localhost:9200'])
        file = {
                "text": str(doc),
                "title": metaname,
            }
        es.index(index='search_index', body = file)
        res = es.search(index='search_index', body={"query": {"match": {"text": query}}}, size=2)
        ans = []
        for hit in res['hits']['hits']:
            ans.append(hit['_source']['text'])
        if metaname in self.redis_client:
                self.retri_dic[metaname] +=1
        else:
            self.retri_dic[metaname] = 1
            if self.retri_dic[metaname] >= 1:
                self.redis_client.lpush(metaname,doc)
                if self.redis_client.llen(metaname) > 10:
                    loc = self.redis_client.lindex(metaname,-1)
                    self.redis_client.ltrim(loc,0,9)
                    self.retri_dic[metaname] = 0
        return ans


    def sym_retrieve(self,db_path,db_name,query,metaname = None):
        logging.basicConfig(filename='results.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        search_path = os.path.join(db_path,db_name)
        chroma_client = chromadb.PersistentClient(path=search_path)
        chroma_collection = chroma_client.get_collection(metaname)

        data = self.redis_client.lrange(loc, 0, -1)
        if data:
            documents = [Document(text=doc_content) for doc_content in data]
            index = VectorStoreIndex.from_documents(documents,embed_model=self.embed_model)
        else:
            if loc is None:
                vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
                index = VectorStoreIndex.from_vector_store(vector_store,embed_model=self.embed_model)
            else:
                # req = {"$contains": loc}
                collection = chroma_collection.get()
                if len(collection['ids']) == 0:
                    raise ValueError('This database doesn\' have file name {}'.format(metaname))
                
                doc = " ".join(collection['documents'])
                documents = [Document(id=doc_id, text=doc_content) for doc_id, doc_content in zip(collection['ids'], collection['documents'])]
                index = VectorStoreIndex.from_documents(documents,embed_model=self.embed_model)

        retriever = VectorIndexRetriever(index=index, similarity_top_k=2)
        result = retriever.retrieve(query)
        ans = result[0].get_content()
        

        if metaname in self.redis_client:
            self.retri_dic[metaname] +=1
        else:
            self.retri_dic[metaname] = 1
            if self.retri_dic[metaname] >= 1:
                self.redis_client.lpush(metaname,doc)
                if self.redis_client.llen(metaname) > 10:
                    loc = self.redis_client.lindex(metaname,-1)
                    self.redis_client.ltrim(loc,0,9)
                    self.retri_dic[metaname] = 0
    
        return ans




    def _create_or_get_collection(self):
        rag_path = os.path.join(self.db_path,self.agent_name, 'rag_data')

        documents = SimpleDirectoryReader(self.data_path).load_data()
        chroma_client = chromadb.PersistentClient(path=rag_path)
        chroma_collection1 = chroma_client.create_collection('rag_data')
        vector_store = ChromaVectorStore(chroma_collection=chroma_collection1)
        storage_context = StorageContext.from_defaults(vector_store=vector_store)
        index_rag = VectorStoreIndex.from_documents(
            documents, storage_context=storage_context, embed_model=self.embed_model
        )
        index = VectorStoreIndex.from_vector_store(vector_store,embed_model=self.embed_model)
        index_rag.storage_context.persist(persist_dir=rag_path)


        inter_path = os.path.join(self.db_path,self.agent_name, 'inter_data')
        os.makedirs(inter_path, exist_ok=True)
        
        chroma_client = chromadb.PersistentClient(path=inter_path)
        chroma_collection = chroma_client.create_collection('inter_data')
        
        vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
        storage_context = StorageContext.from_defaults(vector_store=vector_store)
        
        index_inte = VectorStoreIndex([], storage_context=storage_context, embed_model=self.embed_model)
        
        index_inte.storage_context.persist(persist_dir=inter_path)
        
        doc = chroma_collection1.get(ids=str(chroma_collection1.id))

    
    def check(self):
        inter_path = os.path.join(self.db_path,self.agent_name, 'inter_data')
        chroma_client = chromadb.PersistentClient(path=inter_path)
        collection = chroma_client.get_collection('inter_data')
        do = collection.get()

    
    
    def build_prompt(self,step,ans):
        # context = rag_info + "\n" + inter_info
        step = step
        ans = ans
        prompt_template_literal = (
            "At current step, you need to {step}"
            "<context>\n"
                "{ans}"
            "</context>\n"
        )
        prompt_template = PromptTemplate(prompt_template_literal)
        final_prompt = prompt_template.format(step=step, ans = ans)
        return final_prompt
    

