"""This module contains the BatchQueue class."""
import json
from Queue import Queue
import time
import uuid

from apiclient.errors import HttpError
from apiclient.http import BatchHttpRequest
import httplib2


def rate_limited(max_per_second):
  """A decorator which will throttle a function.

  Args:
    max_per_second: The maximum number of calls allowed per second.

  Returns:
    func: The decorator function.
  """
  min_interval = 1.0 / float(max_per_second)
  def decorate(func):
    """The decorate function."""
    last_time_called = [0.0]
    def rate_limited_function(*args, **kargs):
      """The rate limited function to return."""
      elapsed = time.clock() - last_time_called[0]
      left_to_wait = min_interval - elapsed
      if left_to_wait > 0:
        time.sleep(left_to_wait)
      ret = func(*args, **kargs)
      last_time_called[0] = time.clock()
      return ret
    return rate_limited_function
  return decorate


class BatchQueue(object):
  """Queues requests to Google APIs that are quota limited.

  Attributes:
    quota: The number of simultaneity requests.
    callBack: The callback function to call with the response.
  """

  def __init__(self, quota=10):
    """Creates a BatchQueue object.

    Args:
      quota: The maximum number of simultaneous calls.
    """
    self.quota = quota
    self.call_backs = {}
    self.queue = Queue()
    self.count = 0
    self.start = time.time()

  def call_back(self, request_id, response, exception):
    """The global call_back method for the BatchQueue instance.

    Called when the API responds.
    It keeps track of the number of times the response is called, and if
    the full quota is freed up, it will call execute.

    Args:
      request_id: The request id.
      response: The deserialized response object.
      exception: The apiclient.errors.HttpError exception object if an HTTP
        error occurred while processing the request, or None if no
        error occurred.
    """
    self.count += 1
    if self.count == self.quota:
      self.count = 0
      self.execute()

    callback = self.call_backs[request_id]
    callback(request_id, response, exception)

  def add(self, request, callback, request_id=None):
    """Adds the request to the queue.

    Args:
      request: HttpRequest, Request to add to the batch.
      callback: callable, A callback to be called for this response, of the
        form callback(id, response, exception). The first parameter is the
        request id, and the second is the deserialized response object. The
        third is an apiclient.errors.HttpError exception object if an HTTP error
        occurred while processing the request, or None if no errors occurred.
      request_id: string, A unique id for the request. The id will be passed to
        the callback with the response.
    """

    # Create a unique id if one does not exist.
    if not request_id:
      request_id = str(uuid.uuid4())

    # Add the callback to the dictionary of call backs.
    self.call_backs[request_id] = callback

    # Add the request to the queue.
    self.queue.put((request, request_id))

  @rate_limited(1)
  def execute(self):
    """Executes requests in the queue.

    Removes items from the queue, and adds them to a BatchHttpRequest object.
    Only removes up to set quota. and then calls the BatchHttPRequest object's
    execute method.
    """
    batch = BatchHttpRequest(callback=self.call_back)
    for _ in range(self.quota):
      if self.queue.qsize() == 0:
        break
      request, request_id = self.queue.get()
      batch.add(request, request_id=request_id)

    batch.execute(http=httplib2.Http())

if __name__ == '__main__':

  def call_back(request_id, response, exception):
    if exception is not None:
      if isinstance(exception, HttpError):
        message = json.loads(exception.content)['error']['message']
        print ('Request %s returned API error : %s : %s ' %
               (request_id, exception.resp.status, message))
    else:
      print response

  users = ['liz@gmail.com', 'sue@gmail.com', 'ann@gmail.com']
  batch_queue = BatchQueue(10)
  for user in users:
    # This code assumes you have an authorized analytics service object.
    link = analytics.management().webpropertyUserLinks().insert(
        accountId='XXXXXX',
        webPropertyId='UA-XXXXXX-1',
        body={
            'permissions': {
                'effective': ['READ_AND_ANALYZE'],
                'local': ['READ_AND_ANALYZE'],
            },
            'userRef': {'email': user}
        }
    )
    batch_queue.add(link, call_back)

  for i in range(22):
    # This code assumes you have an authorized analytics service object.
    api_query = analytics.data().ga().get(
        ids='ga:XXXX',
        start_date='2015-01-01',
        end_date='2015-01-30',
        metrics='ga:sessions')
    batch_queue.add(api_query, call_back)

  batch_queue.execute()
