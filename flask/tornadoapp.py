#! /usr/bin/env python

from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop
from tornado.web import FallbackHandler, RequestHandler, Application
from app import app

app.config.from_object(__name__)
app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a123'
app.run(host='0.0.0.0', debug = True)

class MainHandler(RequestHandler):
	def get(self):
		self.write("This message comes from Tornado ^_^")

		tr = WSGIContainer(app)

		application = Application([
				(r"/tornado", MainHandler),
				(r".*", FallbackHandler, dict(fallback=tr)),
				])

		if __name__ == "__main__":
			application.listen(80)
			IOLoop.instance().start()
