from faker import Faker
import pandas as pd

fake = Faker()
data = []

for _ in range(10):
    employee = {
        'id': fake.random_int(min=1, max=100, step=1),
        'name': fake.name(),
        'age': fake.random_int(min=18, max=80, step=1),
        'city': fake.city(),
        'salary': fake.random_int(min=10000, max=100000, step=10)
    }
    data.append(employee)

df = pd.DataFrame(data)
df.to_csv('employees.csv', index=False)