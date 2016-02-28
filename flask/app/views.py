from app import app
from flask import jsonify
from flask import render_template, Flask, flash, request
from wtforms import Form, TextField, TextAreaField, validators, StringField, SubmitField
from subprocess import call
import time as timepackage
import json
import os

class ReusableForm(Form):
    input = TextField('Input:', validators=[validators.required()])


@app.route('/')
def home():
    return render_template("home_static.html")

@app.route('/index')
def index():
    return render_template("home_static.html")

@app.route('/slides')
def slides():
    return render_template("slides_static.html")


@app.route('/input', methods=['GET', 'POST'])
def citycount():
    es = Elasticsearch(hosts=[{"host":"ip-172-31-2-202", "port":9200},{"host":"ip-172-31-2-201", "port":9200},{"host":"ip-172-31-2-200", "port":9200},{"host":"ip-172-31-2-203", "port":9200}] )
    form = ReusableForm(request.form)
    print form.errors

    listof_words_in_es = []
    try:
        with open('words.txt','r') as words:
            for line in words:
                listof_words_in_es.append(line.strip())
    except:
        pass
    if request.method == 'POST':
        input=request.form['input']
        print ' > looking for ' + input +' in the incoming twitterstream. BUT NOT REALLY -> CLUSTERS ARE DOWN'

        if form.validate():
            flash(' >>>>>>>>> looking for ' + input +' in the incoming twitterstream. BUT NOT REALLY -> CLUSTERS ARE DOWN')

        else:
            flash('Error: All the form fields are required. ')
    return render_template("input_static.html", form=form)


@app.route('/output/')
def get_stream():
    return render_template("output_static.html")
