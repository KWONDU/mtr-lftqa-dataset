import json
import torch
from sentence_transformers import SentenceTransformer, util
from tqdm import tqdm
from utils.dataset import load_dataset


def compute_sim(sim_query_list, file_path):
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

    embeddings = []
    for query in tqdm(sim_query_list, desc='Encoding', total=len(sim_query_list)):
        embedding = model.encode(query, convert_to_tensor=True)
        embeddings.append(embedding)

    embeddings = torch.stack(embeddings)

    sim_with_pairs = [
        {
            'query_1_idx': i,
            'query_2_idx': j,
            'similarity': util.pytorch_cos_sim(embeddings[i], embeddings[j]).item()
        }
        for i in tqdm(range(len(sim_query_list)), desc='About ith query', total=len(sim_query_list))
        for j in range(i+1, len(sim_query_list))
    ]

    sorted_sim_with_pairs = sorted(sim_with_pairs, key=lambda x: x['similarity'], reverse=True)

    with open(file_path, 'w') as file:
        json.dump(sorted_sim_with_pairs, file, indent=4)
    
    return sorted_sim_with_pairs


if __name__ == '__main__':
    file_path = 'temp.json'

    qtsumm = load_dataset(dataset_name='QTSumm')

    tables = qtsumm.tables
    train_set = qtsumm.train
    # validation_set = qtsumm.validation
    # test_set = qtsumm.test

    # similarity : header and question (like table retrieval)
    sim_query_list = []
    for data in train_set:  # only train set
        gold_table_id = next(t_id for t_id in data['gold_tables'])
        gold_table = next(t for t in tables if t['id'] == gold_table_id)
        sim_query_list.append(
            f"Gold table's column names are [{', '.join(cn for cn in gold_table['header'])}] and question is '{data['question']}'."
        )

    try:
        with open(file_path, 'r') as file:
            sorted_sim_with_pairs = json.load(file)
    except:
        sorted_sim_with_pairs = compute_sim(sim_query_list=sim_query_list, file_path=file_path)
    
    cnt = 0
    for rank, data in enumerate(sorted_sim_with_pairs):
        query_1_idx = data['query_1_idx']
        query_2_idx = data['query_2_idx']
        sim = data['similarity']

        gold_table_1_id = next(t_id for t_id in train_set[query_1_idx]['gold_tables'])
        gold_table_1 = next(t for t in tables if t['id'] == gold_table_1_id)
        gold_table_2_id = next(t_id for t_id in train_set[query_2_idx]['gold_tables'])
        gold_table_2 = next(t for t in tables if t['id'] == gold_table_2_id)

        if gold_table_1_id == gold_table_2_id:
            continue
        elif gold_table_1['metadata'] == gold_table_2['metadata'] and str(gold_table_1['cell']) != str(gold_table_2['cell']):
            print(f"Rank {rank+1}:\nPage title 1: {gold_table_1['metadata']}\nPage title 2: {gold_table_2['metadata']}\nQuery 1: {sim_query_list[query_1_idx]}\nQuery 2: {sim_query_list[query_2_idx]}\nSim: {sim}\n")
            cnt += 1

        if cnt == 30:
            exit()
