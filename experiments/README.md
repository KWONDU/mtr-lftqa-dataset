# Experiments and evaluation

## Retrieval performance

### Model

    Sparse : BM25
    Dense  : DPR (TAPAS, GPT, BERT embedding), Contriever

### Top-K

    oracle (need at retrieval performance?)
    2 (min)
    5 (max)
    10
    20

### Metric
    precision
    recall
    f1-score
    NDCG
    MAP

## QA (reader) performance

## Model

    > Oracle setting
    MultiTabQA
    LLM (Llama-2, GPT-4)

## Metric

    BLEU
    ROUGE
    METEOR
    BERTScore
    TAPAS-Acc
    G-Eval

## End-to-end performance

    oracle setting? 