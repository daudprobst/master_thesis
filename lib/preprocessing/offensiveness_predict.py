from offensiveness_training import read_germeval_data, SequenceClassificationDataset

import transformers
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from transformers.models.distilbert.modeling_distilbert import DistilBertForSequenceClassification
from transformers.models.distilbert.tokenization_distilbert_fast import DistilBertTokenizerFast
import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
from sklearn.metrics import classification_report, precision_recall_fscore_support

from collections import Counter
from typing import List, Tuple


def load_model(model_filename: str) -> Tuple[DistilBertForSequenceClassification, DistilBertTokenizerFast]:
    """ Loads the model from the specified filename

    :param model_filename: filepath to folder in which the model
    :return: A tuple of (1) the model loaded from the file path and (2) the respective tokenizer
    """
    model = AutoModelForSequenceClassification.from_pretrained(model_filename)
    tokenizer = AutoTokenizer.from_pretrained(model_filename)
    return model, tokenizer


def predict_in_batches(model: transformers.models, tokenizer, dataset: List[str],
                       batch_size: int = 4) -> List[int]:
    """ Predicts the labels for the entries in dataset using the model passed
    :param model: the model to use for prediction
    :param dataset: the values to predict
    :param batch_size: batch size for DataLoader (must be same as for model?)
    :return: list of predictions for dataset
    """
    y_pred = []

    with torch.no_grad():
        model.eval()
        for batch in tqdm(DataLoader(dataset, batch_size=batch_size, shuffle=False)):   
            batch_tokens = tokenizer(batch, return_tensors="pt",
                             padding=True, truncation=True, max_length=64)
            output = model(**batch_tokens)
            y_pred.extend(output.logits.argmax(dim=1).tolist())

    return y_pred


def evaluate_model(model, tokenizer, test_file: str, class_list: List[str], log_errors: bool = False) -> None:
    """Prints out a few evaluations for the model"""

    # load data
    labeled_tweets = read_germeval_data([test_file], CLASS_LIST)

    # tiny bit of preprocessing for bringing tweets into GermEval 2019 structure
    print('Input DataFrame look like this:')
    print(labeled_tweets.head())

    y_true = list(labeled_tweets.label)
    y_pred = predict_in_batches(model, tokenizer, list(labeled_tweets.text))
    
    if log_errors:
        for i, (prediction, true_label) in enumerate(zip(y_pred, y_true)):
            if prediction != true_label:
                print(labeled_tweets.iloc[i].text)
                print(f'Prediction was {CLASS_LIST[prediction]}, true value was {CLASS_LIST[int(true_label)]}\n')

    print(classification_report(y_true, y_pred, labels=[0, 1]))
    f_score_macro = precision_recall_fscore_support(y_true, y_pred, labels=[0, 1], average="macro")[2]
    print(f'F-Score model average (macro): {f_score_macro}')

    print(f'True Distribution: {Counter(y_true)}')
    print(f'Predicted Distribution: {Counter(y_pred)}')


if __name__ == "__main__":
    CLASS_LIST = ['OFFENSE', 'OTHER']

    model, tokenizer = load_model(
        '/home/david/Desktop/Masterarbeit/twit_scrape/models/german_hatespeech_detection_finetuned'
    )

    evaluate_model(model, tokenizer, '/home/david/Desktop/Masterarbeit/twit_scrape/data/aggr_sample/sample_labeled.csv',
                   CLASS_LIST, False)