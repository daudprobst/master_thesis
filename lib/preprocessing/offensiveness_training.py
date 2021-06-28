import torch
import os
import sys
from torch.utils.data import Dataset, DataLoader
from tqdm import tqdm
from collections import namedtuple

import pandas as pd
from sklearn.metrics import precision_recall_fscore_support, classification_report
from sklearn.model_selection import train_test_split
from transformers import AutoModelForSequenceClassification, AutoTokenizer, AdamW, get_linear_schedule_with_warmup

from typing import List


ArgsDesc = namedtuple('ArgsDesc', 'dest num_epochs gradient_accumulation_steps batch_size learning_rate adam_epsilon only_prediction')
ARGS = ArgsDesc(
	dest="/home/david/Desktop/Masterarbeit/twit_scrape/german_hatespeech_detection_finetuned",
	num_epochs=4,
	gradient_accumulation_steps=8,
	batch_size=4,
	learning_rate=2e-5,
	adam_epsilon=1e-8,
	only_prediction=None
)
args = ARGS


class SequenceClassificationDataset(Dataset):
	def __init__(self, x, y, tokenizer):
		self.examples = list(zip(x, y))
		self.tokenizer = tokenizer
		self.device = "cuda" if torch.cuda.is_available() else "cpu"

	def __len__(self):
		return len(self.examples)

	def __getitem__(self, idx):
		return self.examples[idx]

	def collate_fn(self, batch):
		model_inputs = self.tokenizer([i[0] for i in batch], return_tensors="pt",
											padding=True, truncation=True, max_length=64).to(self.device)
		labels = torch.tensor([i[1] for i in batch]).to(self.device)
		return {"model_inputs": model_inputs, "label": labels}


def read_germeval_data(file_paths: List[str], class_list: List[str], label_column: str = 'task1') -> pd.DataFrame:
	""" Reads in and concatenates all the files

	:param file_paths: File paths for files containing training data
	:param class_list: List of labels for transforming that will be used for transforming labels from string to int
	:param label_column: column to load as label (i.e. 'task1' or 'task2')
	:return: A DataFrame containing a 'text' and a 'label' column
	"""

	training_df = pd.concat([
		pd.read_csv(file_path, sep='\t', lineterminator='\n', encoding='utf8', names=["text", "task1", "task2"])
		for file_path in file_paths
	])

	training_df['task1'] = training_df['task1'].str.replace('\r', "")
	training_df['task2'] = training_df['task2'].str.replace('\r', "")

	training_df['label'] = training_df.apply(lambda x: class_list.index(x[label_column]), axis=1)
	# tiny bit of preprocessing to adjust to GermEval text structure (if this is no already present in the data)
	training_df.apply(lambda x: x.replace('\n', '|LBR|'))
	training_df = training_df[['text', 'label']]

	return training_df


def evaluate_epoch(model, dataset):
	model.eval()
	targets = []
	outputs = []
	with torch.no_grad():
		for batch in tqdm(DataLoader(dataset, batch_size=args.batch_size, collate_fn=dataset.collate_fn)):
			output = model(**batch["model_inputs"])
			logits = output.logits
			targets.extend(batch['label'].float().tolist())
			outputs.extend(logits.argmax(dim=1).tolist())

	# Simple False Positives /false negatives function???
	precision_macro, recall_macro, f1_macro, _ = precision_recall_fscore_support(
		targets, outputs, labels=[0,1], average="macro")
	print(f'Precision: {precision_macro}, recall: {recall_macro}, f1_score: {f1_macro}')
	return outputs


def main(model_name: str, train_files: List[str]):
	CLASS_LIST = ['OFFENSE', 'OTHER']
	train_data = read_germeval_data(train_files, CLASS_LIST)

	print(f'First few Rows of data: \n {train_data.head()}')

	X = list(train_data['text'])
	y = list(train_data['label'])

	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

	print(f'Train data has {len(X_train)} vals with {len(y_train)} labels;\n Test data has {len(X_test)}'
		  f' vals with {len(y_test)} labels')

	device = "cuda" if torch.cuda.is_available() else "cpu"
	if device != 'cuda':
		print("No Cuda available!")
	model = AutoModelForSequenceClassification.from_pretrained(model_name)
	tokenizer = AutoTokenizer.from_pretrained(model_name)

	trainset = SequenceClassificationDataset(X_train, y_train, tokenizer)
	devset = SequenceClassificationDataset(X_test, y_test, tokenizer)

	warmup_steps = 0
	train_dataloader = DataLoader(trainset, batch_size=args.batch_size, shuffle=True, collate_fn=trainset.collate_fn)
	t_total = int(len(train_dataloader) * args.num_epochs / args.gradient_accumulation_steps)

	param_optimizer = list(model.named_parameters())
	no_decay = ['bias', 'LayerNorm.bias', 'LayerNorm.weight']
	optimizer_grouped_parameters = [
		{'params': [p for n, p in param_optimizer if not any(nd in n for nd in no_decay)], 'weight_decay': 0.0},
		{'params': [p for n, p in param_optimizer if any(nd in n for nd in no_decay)], 'weight_decay': 0.0}
	]

	optimizer = AdamW(optimizer_grouped_parameters, lr=args.learning_rate, eps=args.adam_epsilon)
	scheduler = get_linear_schedule_with_warmup(optimizer, num_warmup_steps=warmup_steps, num_training_steps=t_total)

	model.zero_grad()
	optimizer.zero_grad()

	use_amp = True
	scaler = torch.cuda.amp.GradScaler(enabled=use_amp)

	if args.only_prediction is not None:
		preds = evaluate_epoch(model, devset)
		save_path = os.path.join(args.dest)
		with open(os.path.join(save_path, "dev_preds.txt"), "w") as f:
			for i in preds:
				f.write(str(i) + "\n")
		sys.exit(0)

	for epoch in range(args.num_epochs):
		model.train()
		t = tqdm(train_dataloader)
		for i, batch in enumerate(t):
			with torch.cuda.amp.autocast(enabled=use_amp):
				output = model(**batch["model_inputs"], labels=batch['label'])
				loss = output.loss / args.gradient_accumulation_steps
			scaler.scale(loss).backward()

			if (i + 1) % args.gradient_accumulation_steps == 0:
				scaler.unscale_(optimizer)
				torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
				scaler.step(optimizer)
				scaler.update()
				scheduler.step()  # Update learning rate schedule
				optimizer.zero_grad()
			acc = (output.logits.argmax(axis=-1).detach() == batch["label"]).float().mean()
			t.set_description(f'Epoch {epoch}, iter {i}, loss: {round(loss.item(), 4)}, acc: {round(acc.item(), 4)}')

		preds = evaluate_epoch(model, devset)
		# Save
		save_path = os.path.join(args.dest)
		os.makedirs(save_path, exist_ok=True)
		tokenizer.save_pretrained(save_path)
		model.save_pretrained(save_path)
	with open(os.path.join(save_path, "dev_preds.txt"), "w") as f:
		for i in preds:
			f.write(str(i) + "\n")


if __name__ == "__main__":
	TRAIN_FILES = [
		'/home/david/Desktop/Masterarbeit/offensiveness_training_data/germeval2018.test_.txt',
		'/home/david/Desktop/Masterarbeit/offensiveness_training_data/germeval2018.training.txt',
		'/home/david/Desktop/Masterarbeit/offensiveness_training_data/germeval2019.training_subtask1_2_korrigiert.txt'
	]

	main(train_files=TRAIN_FILES, model_name='distilbert-base-german-cased')
