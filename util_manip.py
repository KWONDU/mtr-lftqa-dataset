import random


random.seed(42)

def sample_data(data_dict, n=1):
    sample_n_data = dict(random.sample(data_dict.items(), n))
    return sample_n_data
