from src.discourse_style_metrics.offensiveness_training import (
    read_germeval_data,
    CLASS_LIST,
)
import pandas as pd
import transformers
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from transformers.models.distilbert.modeling_distilbert import (
    DistilBertForSequenceClassification,
)
from transformers.models.distilbert.tokenization_distilbert_fast import (
    DistilBertTokenizerFast,
)
import torch
from torch.utils.data import DataLoader
from tqdm import tqdm
from sklearn.metrics import classification_report, precision_recall_fscore_support

from timebudget import timebudget

from collections import Counter
from typing import List, Tuple

import os


def load_model(
    model_filename: str,
) -> Tuple[DistilBertForSequenceClassification, DistilBertTokenizerFast]:
    """Loads the model from the specified filename

    :param model_filename: filepath to folder in which the model
    :return: A tuple of (1) the model loaded from the file path and (2) the respective tokenizer
    """
    model_filename = os.getcwd() + model_filename
    model = AutoModelForSequenceClassification.from_pretrained(model_filename)
    tokenizer = AutoTokenizer.from_pretrained(model_filename)
    return model, tokenizer


def predict_single(model: transformers.models, tokenizer, text: str) -> int:
    token = tokenizer(
        [text], return_tensors="pt", padding=True, truncation=True, max_length=64
    )
    output = model(**token)
    return int(output.logits.argmax(dim=1))


def predict_in_batches(
    model: transformers.models, tokenizer, dataset: List[str], batch_size: int = 4
) -> List[int]:
    """Predicts the labels for the entries in dataset using the model passed
    :param model: the model to use for prediction
    :param tokenizer: the tokenizer used for the model
    :param dataset: the values to predict
    :param batch_size: batch size for DataLoader (must be same as for model?)
    :return: list of predictions for dataset
    """
    y_pred = []

    with torch.no_grad():
        model.eval()
        for batch in tqdm(DataLoader(dataset, batch_size=batch_size, shuffle=False)):
            batch_tokens = tokenizer(
                batch, return_tensors="pt", padding=True, truncation=True, max_length=64
            )
            output = model(**batch_tokens)
            y_pred.extend(output.logits.argmax(dim=1).tolist())

    return y_pred


def evaluate_model(
    model,
    tokenizer,
    labeled_tweets: pd.DataFrame,
    class_list: List[str],
    log_errors: bool = False,
) -> None:
    """Prints out a few evaluations for the model"""
    print("Input DataFrame look like this:")
    print(labeled_tweets.head())

    y_true = list(labeled_tweets.label)
    y_pred = predict_in_batches(model, tokenizer, list(labeled_tweets.text))

    if log_errors:
        for i, (prediction, true_label) in enumerate(zip(y_pred, y_true)):
            if prediction != true_label:
                print(labeled_tweets.iloc[i].text)
                print(
                    f"Prediction was {CLASS_LIST[prediction]}, true value was {CLASS_LIST[int(true_label)]}\n"
                )

    print(classification_report(y_true, y_pred, labels=[0, 1]))
    f_score_macro = precision_recall_fscore_support(
        y_true, y_pred, labels=[0, 1], average="macro"
    )[2]
    print(f"F-Score model average (macro): {f_score_macro}")

    print(f"True Distribution: {Counter(y_true)}")
    print(f"Predicted Distribution: {Counter(y_pred)}")


if __name__ == "__main__":
    model, tokenizer = load_model("models/german_hatespeech_detection_finetuned")

    TRAIN_FILES = [
        "../../data/offensiveness_training_data/germeval2018.test_.txt",
        "../../data/offensiveness_training_data/germeval2018.training.txt",
        "../../data/offensiveness_training_data/germeval2019.training_subtask1_2_korrigiert.txt",
    ]

    full_data = read_germeval_data(TRAIN_FILES, CLASS_LIST)

    X = list(full_data["text"])
    y = list(full_data["label"])

    from sklearn.model_selection import train_test_split

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.33, random_state=42
    )

    test_data_df = pd.DataFrame({"text": X_test, "label": y_test})

    with timebudget("Evaluating model on test set"):
        evaluate_model(model, tokenizer, test_data_df, CLASS_LIST)

    """
    tweets = read_germeval_data(['../../data/aggr_sample/sample_labeled.csv'],
                                CLASS_LIST)

    with timebudget("Predicting in batches"):
        pred_batches = predict_in_batches(model, tokenizer, list(tweets.text))

    with timebudget("Predicting one at a time"):
        pred_singles = []
        for tweet in list(tweets.text):
            pred_singles.append(
                predict_single(model, tokenizer, tweet)
            )

    print(pred_batches == pred_singles)

    print("\n" + str(pred_batches))
    print("\n" + str(pred_singles))
    """
