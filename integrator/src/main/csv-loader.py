import csv, ast, psycopg2

from numpy import long


class CsvLoader:
    def __init__(self):
        pass

    def read_csv(self, file_path):
        f = open(file_path, 'r')
        reader = csv.reader(f)

        longest, headers, type_list = [], [], []

        for row in reader:
            if len(headers) == 0:
                headers = row
                for col in row:
                    longest.append(0)
                    type_list.append('')
            else:
                for i in range(len(row)):
                    # NA is the csv null value
                    if type_list[i] == 'varchar' or row[i] == 'NA':
                        pass
                    else:
                        var_type = self.data_type(row[i], type_list[i])
                        type_list[i] = var_type
                if len(row[i]) > longest[i]:
                    longest[i] = len(row[i])
        statement = 'create table stack_overflow_survey ('

        for i in range(len(headers)):
            if type_list[i] == 'varchar':
                statement = (statement + '\n{} varchar({}),').format(headers[i].lower(), str(longest[i]))
            else:
                statement = (statement + '\n' + '{} {}' + ',').format(headers[i].lower(), type_list[i])

        statement = statement[:-1] + ');'
        f.close()
        return statement

    def data_type(self, val, current_type):
        try:
            # Evaluates numbers to an appropriate type, and strings an error
            t = ast.literal_eval(val)
        except ValueError:
            return 'varchar'
        except SyntaxError:
            return 'varchar'

        if type(t) in [int, long, float]:
            if (type(t) in [int, long]) and current_type not in ['float', 'varchar']:
                # Use smallest possible int type
                if (-32768 < t < 32767) and current_type not in ['int', 'bigint']:
                    return 'smallint'
                elif (-2147483648 < t < 2147483647) and current_type not in ['bigint']:
                    return 'int'
                else:
                    return 'bigint'
            if type(t) is float and current_type not in ['varchar']:
                return 'decimal'
        else:
            return 'varchar'
