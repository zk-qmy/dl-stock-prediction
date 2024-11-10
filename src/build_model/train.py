from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.layers import Dropout
from tensorflow.keras.optimizers import Adam, SGD
from tensorflow.keras.callbacks import ReduceLROnPlateau, EarlyStopping, ModelCheckpoint
import tensorflow as tf
from utils.db_connection import load_config
from utils.logger import Logger


class Trainer:
    def __init__(self):
        self.config = load_config()
        self.model = None
        self.logger = Logger(log_file_name="trainer_log")

    def create_model_lstm(self, input_shape):
        # Define the model
        model = Sequential([
            LSTM(128, input_shape=input_shape, return_sequences=True),
            Dropout(0.2),
            LSTM(64, return_sequences=True),
            Dropout(0.2),
            LSTM(32, return_sequences=False),
            Dense(1)
        ])
        return model

    def compile(self, optimizer):
        optimizer_name = self.config['train']['optimizer']
        learning_rate = self.config['train']['learning_rate']

        if optimizer_name == "adam":
            optimizer = Adam(learning_rate=learning_rate)
        elif optimizer_name == "sgd":
            optimizer = SGD(learning_rate=learning_rate)
        else:
            self.logger.log_error(f"Unsupported optimizer: {optimizer_name}")
            raise ValueError(f"Unsupported optimizer: {optimizer_name}")

        self.model.compile(
            loss=self.config['train']['cont_loss'],
            optimizer=optimizer,
            metrics=['mae']
        )

    def get_callbacks(self, file_path):
        # Training callbacks
        callbacks = [
            EarlyStopping(
                monitor='val_loss',
                patience=self.config["callbacks"]["patience"],
                restore_best_weights=True),
            ModelCheckpoint(filepath=file_path,
                            monitor='val_loss',
                            save_best_only=True),
            ReduceLROnPlateau(monitor='val_loss',
                              factor=self.config["callbacks"]["reduce_lr_factor"],
                              patience=self.config["callbacks"]["reduce_lr_patience"],
                              min_lr=self.config["callbacks"]["min_lr"])
        ]
        self.logger.log_info()
        return callbacks

    def train_history(self, file_path,
                      X_train_norm, y_train_norm,
                      X_val_norm, y_val_norm):
        if self.model is None:
            self.logger.log_error("Model is not created.")
            raise ValueError("Model is not created.")

        callbacks = self.get_callbacks(file_path)
        self.logger.log_info("Start training...")
        # Fit the model with data augmentation
        model_training_history = self.model.fit(
            X_train_norm, y_train_norm,
            batch_size=self.config["train"]["batch_size"],
            epochs=self.config["train"]["epochs"],
            validation_data=(X_val_norm, y_val_norm),
            callbacks=callbacks
        )
        self.logger.log_info("Training Completed!")
        return model_training_history
