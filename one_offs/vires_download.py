import os

from viresclient import AeolusRequest

request = AeolusRequest(token=os.environ["VIRES_TOKEN"], url="https://aeolus-portal.eu/viresclient")

request.set_collection("ALD_U_N_2B")
print(request.print_available_collections())

data = request.get_between("2020-10-01T00:00:00.000Z", "2020-10-03T00:00:00.000Z")
print(data)
