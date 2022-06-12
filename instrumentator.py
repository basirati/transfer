'''
This module provides the mechanisms to easily add monitoring to an app endpoints using Python decorators.
'''
import functools
import json

# If Instrument class is used for FastAPI and there is no Flask installed,
# it will pass the import
try:
	from flask import request
except ImportError:
	pass

from prometheus_client import Counter, Histogram, Gauge, generate_latest


def transform_request_to_json_flask(request, **kwargs):
	'''
    Transforms the Flask request object to a JSON
    :param request: the request object
	:param **kwargs: the passed parameters to the main decorated function
    :returns: request parameters as a JSON 
    '''
	if len(kwargs.keys()):
		return kwargs
	res = dict()
	#POST
	if request.method == 'POST':
		params = request.values
	#GET
	else:
		params = request.args
	for key in params.keys():
		res[key] = params.get(key)
	return res

def transform_request_to_json_fastapi(request, **kwargs):
	'''
    Transforms the FastAPI request object to a JSON
    :param request: value is not important, it's only for the sake of consistency with Flask method signature
	:param *kwargs: the passed parameters to the main decorated function
    :returns: request parameters as a JSON 
    '''
	return kwargs

def transform_response_to_json_flask(resp_obj):
	'''
    Transforms the Flask response object to a JSON
    :param resp_obj: the response object
    :returns: main results as a JSON 
    '''
	txt_resp = resp_obj.get_data(as_text=True)
	return json.loads(txt_resp)

def transform_response_to_json_fastapi(resp_obj):
	'''
    Transforms the FastAPI response object to a JSON
    :param resp_obj: the response object
    :returns: main results as a JSON 
    '''
	if isinstance(resp_obj, dict):
		return resp_obj
	else:
		body = resp_obj.body.decode()
		return json.loads(body)

def search_json(key, json_obj):
	'''
	Searches a json or a dict for finding the value of a key, if the key is not available, it returns None
	:param key: the key that it searches for
	:param json_obj: the json-like object that will be searched
	:return either value of the found key or None
	'''
	try:
		for ikey in json_obj:
			if ikey == key:
				return json_obj[key]
			res = search_json(key, json_obj[ikey])
			if not res is None:
				return res
		return None
	except:
		return None

class Instrumentation():
	'''
    The general class for instrumentation of an app using Python decorators. 
	It provides all basic decorators to monitor an app endpoints.
    '''
	def __init__(self, app) -> None:
		'''
    	Initializes the class by the app. It identifies whether the app is Flask or FastAPI.
    	:param app: the main app object
    	'''
		self.app = app
		## Checking whether the app is a Flask app
		if self.app.__class__.__name__ == 'Flask':
			self.transform_request = transform_request_to_json_flask
			self.transform_response = transform_response_to_json_flask
			@self.app.route('/metrics')
			def metrics():
				return generate_latest()
		## Checking whether the app is a FastAPI app
		elif self.app.__class__.__name__ == 'FastAPI':
			self.transform_request = transform_request_to_json_fastapi
			self.transform_response = transform_response_to_json_fastapi
			@self.app.get('/metrics')
			def metrics():
				return generate_latest()
		else:
			raise Exception('The instrumentation does not support the app of type:' 
				+ self.app.__class__.__name__)

	## Feedback metrics ---------------------------------------------------

	def count_false_binary_feedback(self, id, feedback_key, threshold_key=None):
		'''
		Creates two counters, one for false positive and one for false negative of a binary classification.
		It assumes that the feedback has a corrctness and a corrected_value attribute
   		:param id: the id with which the metric must be recognized and differentiated
		:param feedback_key: the feedback key in the request
		:param threshold_key: (optional) deciding on positive and negative according to the threshold, if not given, threshold is 0.5
    	:returns: the decorator function
    	'''
		## False Positive Counter
		fp_counter = Counter(id+'_fp', 'Counter for false positive:'+id)
		## False Negative Counter
		fn_counter = Counter(id+'_fn', 'Counter for false negative:'+id)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				request_json = self.transform_request(request, **kwargs)
				feedback = search_json(feedback_key, request_json)
				if threshold_key:
					threshold = search_json(threshold_key, request_json)
				else:
					threshold = 0.5
				if feedback.correctness < 0:
					if feedback.corrected_value > threshold:
						fn_counter.inc()
					else:
						fp_counter.inc()
				return resp_obj
			return wrapper
		return decorator

	## Feature metrics -----------------------------------------------------

	def count_feature(self, id, feature, *values):
		'''
		Creates the metric for counting the values of a feature over time using Histogram.
   		:param id: the id with which the metric must be recognized and differentiated
		:param feature: the feature key in the request
		:param *values: the values which we count their occurence for the feature
    	:returns: the decorator function
    	'''
		#counters keeps the metric for each value, feature name+value as keys and metrics as values
		counters = dict()
		#Creating a metric for each feature and recording it in counters
		for val in values:
			counters[feature+'_'+str(val)] = Counter(id+'_'+feature+'_'+str(val), 'Counter for feature and value:'+feature+'_'+str(val))
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				request_json = self.transform_request(request, **kwargs)
				for val in values:
					if request_json[feature] == val:
						counters[feature+'_'+str(val)].inc()
						break
				return resp_obj
			return wrapper
		return decorator

	def gauge_feature(self, id, *feature_keys):
		'''
		Creates the metric for monitoring the values of a feature over time using Gauge.
   		:param id: the id with which the metric must be recognized and differentiated
		:param feature_keys: the keys of the features in the request
    	:returns: the decorator function
    	'''
		#gauges keeps the metric for each feature, feature names as keys and metrics as values
		gauges = dict()
		#Creating a metric for each feature and recording it in gauges
		for feature in feature_keys:
			gauges[feature] = Gauge(id+'_'+feature, 'Gauge metric for feature:'+feature)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				request_json = self.transform_request(request, **kwargs)
				for feature in feature_keys:
					gauges[feature].set(request_json[feature])
				return resp_obj
			return wrapper
		return decorator

	def hist_feature(self, id, buckets, *feature_keys):
		'''
		Creates the metric for monitoring the values of a feature over time using Histogram.
   		:param id: the id with which the metric must be recognized and differentiated
		:param buckets: the list of values which define the buckets used for creating the histogram
		:param feature_keys: the keys of the features in the request
    	:returns: the decorator function
    	'''
		#hists keeps the metric for each feature, feature names as keys and metrics as values
		hists = dict()
		#Creating a metric for each feature and recording it in hists
		for feature in feature_keys:
			hists[feature] = Histogram(id+'_'+feature, 'Histogram for feature:'+feature, buckets=buckets)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				request_json = self.transform_request(request, **kwargs)
				for feature in feature_keys:
					hists[feature].observe(request_json[feature])
				return resp_obj
			return wrapper
		return decorator

	##ML Metrics ------------------------------------------------------------------
	
	def count_binary(self, id, result_key, threshold_key=None):
		'''
    	Creates two counter metrics for counting positive and negative classes of a binary prediction.
   		:param id: the id with which the metric must be recognized and differentiated
		:param result_key: the key of the result in the response JSON
		:param threshold_key: (optional) positive and negative classes are decided based on threshold, if not given threshold is 0.5
    	:returns: the decorator function
    	'''
		#Counter metric for the positive class
		positive_cls = Counter(id+'_pos', 'Counter metric for the positive class of:'+id)
		#Counter metric for the negative class
		negative_cls = Counter(id+'_neg', 'Counter metric for the negative class of:'+id)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				resp_json = self.transform_response(resp_obj)
				value = search_json(result_key, resp_json)
				if threshold_key:
					threshold = search_json(threshold_key, resp_json)
				else:
					threshold = 0.5
				if not value is None:
					if value > threshold:
						positive_cls.inc()
					else:
						negative_cls.inc()
				return resp_obj
			return wrapper
		return decorator
		
	def count_classes(self, id, threshold_key=None, **class_keys):
		'''
    	Creates as one counter metric per class for monitoring a multi class prediction.
   		:param id: the id with which the metric must be recognized and differentiated
		:param *class_keys: keys(names) of each class
		:param threshold_key: (optional) based on which we decide positve and negative for a class, if not given threshold is 0.5
    	:returns: the decorator function
    	'''
		#counters keeps the metric for each class, class name for key and metric for value
		counters = dict()
		#Creating a counter metric for each class and recording it in counters
		for cls in class_keys:
			counters[cls] = Counter(id+'_'+cls, 'Counter metric for the class:'+cls)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				resp_json = self.transform_response(resp_obj)
				if threshold_key:
					threshold = search_json(threshold_key, resp_json)
				else:
					threshold = 0.5
				for cls in class_keys:
					value = search_json(cls, resp_json)
					if not value is None:
						if value > threshold:
							counters[cls].inc()
				return resp_obj
			return wrapper
		return decorator
		
	
	def gauge_output(self, id, *value_keys):
		'''
    	Creates the a gauge metric to monitor an output value at each point of the time.
   		:param id: the id with which the metric must be recognized and differentiated
		:param *value_keys: the keys in the response JSON that return the corresponding values for monitoring
    	:returns: the decorator function
    	'''
		#gauges keeps the metrics for each output value, output names as keys and metrics as values
		gauges = dict()
		#Creating a metric for each output and recording it in gauges
		for key in value_keys:
			gauges[key] = Gauge(id+'_'+key, 'Gauge metric for:'+key)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				resp_json = self.transform_response(resp_obj)
				for key in value_keys:
					value = search_json(key, resp_json)
					if not value is None:
						gauges[key].set(value)
				return resp_obj
			return wrapper
		return decorator
		

	def hist_output(self, id, buckets, *value_keys):
		'''
    	Creates the historgram metric for an output through recording its values in a hist.
   		:param id: the id with which the metric must be recognized and differentiated
		:param buckets: a list of values that define the buckets based on which the histogram must be built and record the values
		:param *value_keys: the keys in the response JSON that return the corresponding values for monitoring
    	:returns: the decorator function
    	'''
		#hists keeps the metrics for each output value, output names as keys and metrics as values
		hists = dict()
		#Creating a metric for each output and recording it in hists
		for key in value_keys:
			hists[key] = Histogram(id+'_'+key, 'Histogram for:'+key, buckets=buckets)
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				resp_json = self.transform_response(resp_obj)
				for key in value_keys:
					value = search_json(key, resp_json)
					if not value is None:
						hists[key].observe(value)
				return resp_obj
			return wrapper
		return decorator

	def hist_output_specific(self, id, **buckets):
		'''
    	Creates the historgram metric, based on specific buckets for each output value, through recording its values in a hist.
   		:param id: the id with which the metric must be recognized and differentiated
		:param **buckets: a dictionary with output names as keys and buckets (list) as values. Each bucket is specifically used for its corresponding output.
    	:returns: the decorator function
    	'''
		#hists keeps the metrics for each output value, output names as keys and metrics as values
		hists = dict()
		#Creating a metric for each output and recording it in hists
		for val in buckets.keys():
			hists[val] = Histogram(id+'_'+val, 'Histogram for:'+val, buckets=buckets[val])
		def decorator(func):
			@functools.wraps(func)
			def wrapper(*args, **kwargs):
				resp_obj = func(*args, **kwargs)
				resp_json = self.transform_response(resp_obj)
				for key in buckets.keys():
					value = search_json(key, resp_json)
					if not value is None:
						hists[key].observe(value)
				return resp_obj
			return wrapper
		return decorator