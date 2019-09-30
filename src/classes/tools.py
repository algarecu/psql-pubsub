import json

class MultiDimensionalArrayEncoder(json.JSONEncoder):
    def encode(self, obj):
        def hint_tuples(item):
            if isinstance(item, tuple):
                return {'__tuple__': True, 'items': item}
            if isinstance(item, list):
                return [hint_tuples(e) for e in item]
            if isinstance(item, dict):
                return {key: hint_tuples(value) for key, value in item.items()}
            else:
                return item

        return super(MultiDimensionalArrayEncoder, self).encode(hint_tuples(obj))

def hinted_tuple_hook(obj):
    if '__tuple__' in obj:
        return tuple(obj['items'])
    else:
        return obj

