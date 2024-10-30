from flask import Flask

app = Flask(__name__)

# Enter Corrections AKA Home
@app.route('/')
def enterCorrections():
    return '<h1>Hello, World!</h1>' # Link html templates here instead..

#View Ghcn Data and Metadata
@app.route('/viewGhcnData/')
def viewGhcnData():
    return '<h3>This is a Flask web application.</h3>'

#Resources
@app.route('/resources/readme')
def readme():
    return '<h3>This is a Flask web application.</h3>'

@app.route('/resources/countrycodes')
def countryCodes():
    return '<h3>This is a Flask web application.</h3>'

@app.route('/resources/classdefinitions')
def classDefinitions():
    return '<h3>This is a Flask web application.</h3>'

@app.route('/login/')
def login():
    return '<h3>This is a Flask web application.</h3>'

