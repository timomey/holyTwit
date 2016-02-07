#!/usr/bin/env python

from app import app
#app = Flask(__name__)
app.config.from_object(__name__)
app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a123'
app.run(host='0.0.0.0', debug = True)
