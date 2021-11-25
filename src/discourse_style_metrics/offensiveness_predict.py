import os
from collections import Counter
from typing import List, Tuple

import pandas as pd
import torch
import transformers
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.model_selection import train_test_split
from timebudget import timebudget
from torch.utils.data import DataLoader
from tqdm import tqdm
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from transformers.models.distilbert.modeling_distilbert import (
    DistilBertForSequenceClassification,
)
from transformers.models.distilbert.tokenization_distilbert_fast import (
    DistilBertTokenizerFast,
)

from src.discourse_style_metrics.offensiveness_training import (
    CLASS_LIST,
    read_germeval_data,
)


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
                    f"Prediction was {class_list[prediction]}, true value was {class_list[int(true_label)]}\n"
                )

    print(classification_report(y_true, y_pred, labels=[0, 1]))
    f_score_macro = precision_recall_fscore_support(
        y_true, y_pred, labels=[0, 1], average="macro"
    )[2]
    print(f"F-Score model average (macro): {f_score_macro}")

    print(f"True Distribution: {Counter(y_true)}")
    print(f"Predicted Distribution: {Counter(y_pred)}")


if __name__ == "__main__":
    model, tokenizer = load_model("/models/german_hatespeech_detection_finetuned")

    TRAIN_FILES = [
        f"{os.getcwd()}/data/offensiveness_training_data/germeval2018.test_.txt",
        f"{os.getcwd()}/data/offensiveness_training_data/germeval2018.training.txt",
        f"{os.getcwd()}/data/offensiveness_training_data/germeval2019.training_subtask1_2_korrigiert.txt",
    ]

    full_data = read_germeval_data(TRAIN_FILES, CLASS_LIST)

    X = list(full_data["text"])
    y = list(full_data["label"])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.33, random_state=42
    )

    test_data_df = pd.DataFrame({"text": X_test, "label": y_test})

    with timebudget("Evaluating model on test set"):
        evaluate_model(model, tokenizer, test_data_df, CLASS_LIST)
