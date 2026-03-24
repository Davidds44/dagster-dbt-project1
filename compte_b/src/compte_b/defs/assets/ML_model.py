import dagster as dg
import mlflow

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, classification_report
#from sklearn.feature_extraction import text
from nltk.corpus import stopwords
#from spacy.lang.fr.stop_words import STOP_WORDS as fr_stop
import re
# A tester :
from sklearn.feature_extraction.text import CountVectorizer


# @dg.asset(
#     name="ML_model",
#     deps=["stg_classed_data"]
#     required_resource_keys={"database"}
# )
# def ML_model(context: dg.AssetExecutionContext, database: DuckDBConnection) -> dg.MaterializeResult:
    # # Load the data
    # data = database.query("SELECT type, libelle_clean FROM stg_classed_data").df()
    # # Split the data into training and testing sets
    # X_train, X_test, y_train, y_test = train_test_split(data["libelle_clean"], data["type"], test_size=0.2, random_state=42)

    # # Vectorize the text data
    # my_stop_words = stopwords.words('french') + ['CB', 'REF', 'ref', 'cb', 'SAS', 'sas', 'CARTE', 'NUMERO', 'EUR', 'REFERENCE']
    # tfidf_vectorizer = TfidfVectorizer(max_features=5000,stop_words=my_stop_words, ngram_range=(1,3))  # You can adjust the number of features as needed

    # X_train_tfidf = tfidf_vectorizer.fit_transform(X_train)
    # X_test_tfidf = tfidf_vectorizer.transform(X_test)

    # # Initialize and train the Gradient Boosting Classifier
    # clf = GradientBoostingClassifier(n_estimators=100, random_state=42)  # You can adjust the hyperparameters
    # clf.fit(X_train_tfidf, y_train)

    # # Make predictions
    # y_pred = clf.predict(X_test_tfidf)

    # # Evaluate the model
    # accuracy = accuracy_score(y_test, y_pred)
    # report = classification_report(y_test, y_pred)

    # return dg.MaterializeResult(
    #     metadata={
    #         "accuracy": dg.MetadataValue.float(accuracy),
    #         "report": dg.MetadataValue.json(report),
    #     }
    # )