"""A simple example of Google Analytics batched user permissions."""
import json
from apiclient.errors import HttpError
from apiclient.http import BatchHttpRequest

def call_back(request_id, response, exception):
  """Handle batched request responses."""
  print request_id
  if exception is not None:
    if isinstance(exception, HttpError):
      message = json.loads(exception.content)['error']['message']
      print ('Request %s returned API error : %s : %s ' %
             (request_id, exception.resp.status, message))
  else:
    print response


def add_users(users, permissions):
  """Adds users to every view (profile) with the given permissions.

  Args:
    users: A list of user email addresses.
    permissions: A list of user permissions.
  Note: this code assumes you have MANAGE_USERS level permissions
  to each profile and an authorized Google Analytics service object.
  """

  # Get the a full set of account summaries.
  account_summaries = analytics.management().accountSummaries().list().execute()

  # Loop through each account.
  for account in account_summaries.get('items', []):
    account_id = account.get('id')

    # Loop through each user.
    for user in users:
      # Create the BatchHttpRequest object.
      batch = BatchHttpRequest(callback=call_back)

      # Loop through each property.
      for property_summary in account.get('webProperties', []):
        property_id = property_summary.get('id')

        # Loop through each view (profile).
        for view in property_summary.get('profiles', []):
          view_id = view.get('id')

          # Construct the Profile User Link.
          link = analytics.management().profileUserLinks().insert(
              accountId=account_id,
              webPropertyId=property_id,
              profileId=view_id,
              body={
                  'permissions': {
                      'local': permissions
                  },
                  'userRef': {
                      'email': user
                  }
              }
          )
          batch.add(link)

      # Execute the batch request for each user.
      batch.execute()

if __name__ == '__main__':

  # Construct a list of users.
  emails = ['ona@gmail.com', 'emi@gmail.com', 'sue@gmail.com', 'liz@gmail.com']

  # call the add_users function with the list of desired permissions.
  add_users(emails, ['READ_AND_ANALYZE'])
