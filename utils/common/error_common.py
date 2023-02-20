class ErrorBase(Exception):
    def __init__(self, *args, **kwargs):
        super(ErrorBase, self).__init__(*args, **kwargs)

    def __str__(self):
        return "%s[%s]: %s" % (self.MSG, self.CODE, self.detail)

    def to_dict(self):
        return {'code': self.CODE, 'msg': '%s: %s' % (self.MSG, self.detail), 'data': getattr(self, 'data', None)}


class ErrorHintBase(Exception):
    def __init__(self, *args, **kwargs):
        super(ErrorBase, self).__init__(*args, **kwargs)

    def __str__(self):
        return "%s[%s]: %s" % (self.MSG, self.CODE, self.detail)

    def to_dict(self):
        return {'code': self.CODE, 'msg': '操作成功' % (self.MSG, self.detail),
                'data': getattr(self, 'data', None), 'tip': '%s: %s' % (self.MSG, self.detail)}



class CommonError(ErrorBase):
    # CODE = 10000
    MSG = "失败"

    def __bool__(self):
        return False

    def __init__(self, detail=None, code=10000):
        self.CODE = code
        self.detail = detail

    def to_dict(self):
        return {'code': self.CODE,
                'msg': '%s' % self.detail,
                'data': getattr(self, 'data', None),
                'datalist': getattr(self, 'datalist', None)}
