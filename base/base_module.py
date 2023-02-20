from playhouse.pool import PooledPostgresqlDatabase
from peewee import *
from peewee import Expression, FIELD, NamespaceAttribute
from playhouse.postgres_ext import ArrayField
from base.config import cfg
from datetime import datetime, date
from typing import Any, List, Dict
from collections import defaultdict
from copy import deepcopy

class GcDb(PooledPostgresqlDatabase):
    def __init__(self, *args, **kwargs):
        super(GcDb, self).__init__(*args, **kwargs)


db = GcDb(
    database=cfg.db_database,
    host=cfg.db_addr,
    port=cfg.db_port,
    user=cfg.db_username,
    password=cfg.db_password,
    max_connections=300,
    stale_timeout=300,
)


class OperatorHelper:
    def __init__(self, operator):
        MAP_OP = {
            "=": OperatorHelper.peewee_eq,
            "!=": OperatorHelper.peewee_neq,
            "like": OperatorHelper.peewee_like,
            "ilike": OperatorHelper.peewee_ilike,
            "in": OperatorHelper.peewee_in,
            "not_in": OperatorHelper.peewee_not_in,
            ">": OperatorHelper.peewee_gt,
            "<": OperatorHelper.peewee_lt,
            ">=": OperatorHelper.peewee_egt,
            "<=": OperatorHelper.peewee_elt,
            "@>": OperatorHelper.peewee_contains,
        }
        self._func = MAP_OP[operator]

    def __call__(self, left, right):
        return self._func(left, right)

    @staticmethod
    def peewee_eq(left, right):
        return left == right

    @staticmethod
    def peewee_neq(left, right):
        return left != right

    @staticmethod
    def peewee_like(left, right):
        return left % ("%%%s%%" % right)

    @staticmethod
    def peewee_in(left, right):
        return left.in_(right)

    @staticmethod
    def peewee_not_in(left, right):
        return left.not_in(right)

    @staticmethod
    def peewee_ilike(left, right):
        return left ** ("%%%s%%" % right)

    @staticmethod
    def peewee_gt(left, right):
        return left > right

    @staticmethod
    def peewee_lt(left, right):
        return left < right

    @staticmethod
    def peewee_egt(left, right):
        return left >= right

    @staticmethod
    def peewee_elt(left, right):
        return left <= right

    @staticmethod
    def peewee_contains(left, right):
        return left.contains(right)


class AutoQueryHelper:
    def __init__(self, model):
        self.TABLE_MODEL_MAP = db
        self.model = model
        self.query = model.select()
        self.models_joined = []       # 已经join过的model列表
        self.last_model = None        # 最后一个JOIN的model
        self.where_expression = []    # where条件
        self.order_by_expression = []   # order_by条件
        self.distinct = False

    def auto_join(self, join_attributes):
        for join_attr in join_attributes:
            if getattr(self.last_model, join_attr, None) is not None:
                # 外键JOIN
                this_column_name = getattr(self.last_model, join_attr).column_name
                model_wait_join = getattr(self.last_model, join_attr).rel_model
                if model_wait_join not in self.models_joined:
                    self.query = (self.query.join(model_wait_join, on=(getattr(self.last_model, this_column_name) == getattr(model_wait_join, model_wait_join._meta.primary_key.column_name))))
                    self.models_joined.append(model_wait_join)
                self.last_model = model_wait_join
            else:
                # 垂直拆分表JOIN 一般都是id与id一一对应
                model_wait_join = self.TABLE_MODEL_MAP[join_attr]
                if model_wait_join not in self.models_joined:
                    if getattr(model_wait_join, self.last_model._meta.table_name, None):
                        self.query = self.query.join(
                            model_wait_join,
                            on=(getattr(self.last_model, self.last_model._meta.primary_key.column_name) == getattr(model_wait_join, self.last_model._meta.table_name))
                        )
                    else:
                        from peewee import ForeignKeyField, FieldAccessor
                        for k, v in model_wait_join.__dict__.items():
                            if isinstance(v, FieldAccessor) and isinstance(v.field, ForeignKeyField):
                                self.query = self.query.join(
                                    model_wait_join,
                                    on=(
                                    getattr(self.last_model, self.last_model._meta.primary_key.column_name) == getattr(model_wait_join, k))
                                )
                                break
                    self.models_joined.append(model_wait_join)
                self.last_model = model_wait_join
                self.distinct = True

    def generate_query(self, where_domain, order_by=None):
        # where条件JOIN
        print(where_domain)
        self.get_where_or_order_by(where_domain, order_by)
        self.order_by_expression.append(self.model._meta.primary_key.desc())
        if len(self.where_expression) > 0:
            self.query = self.query.where(*self.where_expression)
        self.query = self.query.order_by(*self.order_by_expression)
        if self.distinct:
            self.query = self.query.distinct()
        return self.query

    def get_where_or_order_by(self, where_domain, order_by):
        if where_domain is not None:
            for item in where_domain:
                # all_attributes.add(item[0])
                join_attributes = item[0].split('.')
                if len(join_attributes) > 1:
                    # 说明需要JOIN
                    last_attr = join_attributes.pop()
                    self.last_model = self.model  # 每一次循环都是从当前model开始
                    self.auto_join(join_attributes)  # 自动JOIN，每一次JOIN都会改变last_model
                    this_where_expr = OperatorHelper(operator=item[1])(left=getattr(self.last_model, last_attr),
                                                                       right=item[2])
                    self.where_expression.append(this_where_expr)
                else:
                    # 不需要join
                    this_where_expr = OperatorHelper(operator=item[1])(left=getattr(self.model, item[0]), right=item[2])
                    self.where_expression.append(this_where_expr)
        # order_by 可能也需要JOIN
        if order_by is not None:
            for _order in order_by:
                sort_type = _order.split(' ')[1].lower()
                assert sort_type in ['asc', 'desc']
                left_str = _order.split(' ')[0]
                join_attributes = left_str.split('.')
                if len(join_attributes) > 1:
                    last_attr = join_attributes.pop()
                    self.last_model = self.model
                    self.auto_join(join_attributes)
                    self.order_by_expression.append(getattr(getattr(self.last_model, last_attr), sort_type)())
                else:
                    field = getattr(self.model, left_str)
                    self.order_by_expression.append(getattr(field, sort_type)())
        return self.where_expression, self.order_by_expression

    def get_field_list(self, fields):
        res = {}
        for item in fields:
            join_attributes = item.split('.')
            if len(join_attributes) > 1:
                # 说明需要JOIN
                last_attr = join_attributes[1]
                last_model = join_attributes[0]
                this_column_name = getattr(self.model, last_model).column_name
                model_wait_join = getattr(self.model, last_model).rel_model

                self.query = (self.query.join(model_wait_join, on=(
                            getattr(self.model, this_column_name) == getattr(model_wait_join,
                                                                                  model_wait_join._meta.primary_key.column_name))))

                res[item] = list(set([getattr(getattr(query, last_model), last_attr) for query in self.query]))
            else:
                # 不需要join
                res[item] = list(set([getattr(query, item) for query in self.query]))
            self.query = self.model.select()
        return res



class QueryHelper(object):
    def __init__(self, model):
        self.model = model
        self.query = model.select()

    def __iter__(self):
        return self.query.__iter__()

    def __len__(self):
        return self.query.count()

    def get_all(self):
        return self.query

    def get_first(self):
        return self.query.first()

    def get_count(self):
        return self.query.count()

    def _parse_kwargs(self, kwargs):
        filters = []
        for k, v in kwargs.items():
            if v is None:
                continue
            attr = getattr(self.model, k, None)
            if attr is not None:
                if isinstance(v, list):
                    filters.append(attr.in_(v))  # in
                elif isinstance(v, tuple):
                    if len(v) == 1 and v[0] is not None:
                        filters.append(attr ** ("%%%s%%" % v[0]))  # ilike 单个元素的tuple代表模糊查询
                    elif len(v) == 2:
                        pass
                else:
                    filters.append(attr == v)
        return filters

    def and_(self, **kwargs):
        filters = self._parse_kwargs(kwargs)
        and_list = None
        for one in filters:
            and_list = one & and_list if and_list else one
        return and_list

    def or_(self, **kwargs):
        filters = self._parse_kwargs(kwargs)
        or_list = None
        for one in filters:
            or_list = one | or_list
        return or_list

    def filter(self, *condition):
        and_list = None
        for one in condition:
            if isinstance(one, Expression):
                and_list = one & and_list if and_list else one
        self.query = self.query.where(and_list)
        return self

    def where(self, **kwargs):
        filters = self._parse_kwargs(kwargs)
        if filters:
            filters = tuple(filters)
            self.query = self.query.where(*filters)
        return self

    def page(self, limit, page):
        limit = int(limit) if limit else 0
        page = int(page) if page else 1
        offset = (page - 1) * limit
        if offset:
            self.query = self.query.offset(offset)
        if limit:
            self.query = self.query.limit(limit)
        return self

    def offset(self, offset):
        if offset:
            self.query = self.query.offset(offset)
        return self

    def limit(self, limit):
        if limit:
            self.query = self.query.limit(limit)
        return self

    def desc(self, desc):
        attr = getattr(self.model, desc, None)
        if attr:
            self.query = self.query.order_by(attr.desc())
        return self

    def asc(self, asc):
        attr = getattr(self.model, asc, None)
        if attr:
            self.query = self.query.order_by(attr.asc())
        return self

    def distinct(self, distinct):
        attr = getattr(self.model, distinct, None)
        if attr:
            self.query = self.query.distinct(attr)
        return self

    def order_by(self, order_by):
        if not isinstance(order_by, dict):
            return self
        order_by_list = []
        for column_name, sequence in order_by.items():
            column = getattr(self.model, column_name, None)
            if column:
                order_sql = column.asc() if sequence.lower() == "asc" else column.desc()
                order_by_list.append(order_sql)
        if order_by_list:
            self.query = self.query.order_by(*order_by_list)
        return self


class SqlNone(object):
    def __init__(self, model, id):
        self.model = model
        self.id = id
        self.item = None

    def __bool__(self):
        return False

    def __call__(self, *args, **kwargs):
        s = "try to call %s(%s, %s) from None(%s[%s])" % (self.item, args, kwargs, self.model.__name__, self.id)
        raise s
        # print(s)
        # if self.item.startswith('to_dict'):
        #     return {}
        # else:
        #     return s

    def __getattr__(self, item):
        s = "try to get '%s' from None(%s[%s])" % (item, self.model.__name__, self.id)
        # raise sql_error.SelectError(s)
        # self.item = item
        # member = getattr(self.model, item, None)
        # print("SqlNone:", self.model, item, member)
        # if callable(member):
        #     return self
        # return "try to get '%s' from None(%s[%s])" % (item, self.model.__name__, self.id)


class SafeUpdateHelper(object):
    def __init__(self, model):
        self.model = model

    def __setattr__(self, key, value):
        if key == "model":
            return super(SafeUpdateHelper, self).__setattr__(key, value)
        check_func = getattr(self.model, "safe_update_" + key, None)
        check_func(value)


class BaseModel(Model):
    class Meta:
        database = db
        order_by = 'gen_time desc'

    def to_dict(self, *keys, **key_alias):
        d = {}
        if not keys and not key_alias:
            keys = self._meta.fields.keys()
        for k in keys:
            value = getattr(self, k)
            if isinstance(self._meta.fields[k], DateTimeField) and value is not None:
                d[k] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(self._meta.fields[k], DateField) and value is not None:
                d[k] = value.strftime("%Y-%m-%d")
            elif isinstance(self._meta.fields[k], ForeignKeyField):
                # d[k] = value.to_dict()
                d[self._meta.fields[k].column_name] = value and value.get_id()
            else:
                d[k] = value
        for k, alias in key_alias.items():
            value = getattr(self, k)
            if isinstance(value, datetime):
                d[alias] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, BaseModel):
                # d[k] = value.to_dict()
                d[self._meta.fields[k].column_name] = value.get_id()
            else:
                d[alias] = value
        return d

    def model_serializer(self, *keys, **key_alias):
        d = {}
        if not keys and not key_alias:
            keys = self._meta.fields.keys()
        for k in keys:
            column_name = self._meta.fields[k].column_name
            value = getattr(self, column_name)
            if isinstance(self._meta.fields.get(column_name), DateTimeField) and value is not None:
                d[column_name] = value and value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(self._meta.fields.get(column_name), DateField) and value is not None:
                d[column_name] = value and value.strftime("%Y-%m-%d")
            else:
                d[column_name] = value
        for k, alias in key_alias.items():
            column_name = self._meta.fields[k].column_name
            value = getattr(self, column_name)
            if isinstance(value, datetime):
                d[alias] = value.strftime("%Y-%m-%d %H:%M:%S")
            else:
                d[alias] = value
        return d

    def to_dict_exclude(self, exclude=None, **key_alias):
        """
        :param exclude: 不需要序列化的字段名 例: exclude=['age', 'school', 'country']
        :param key_alias: 给序列化字段重新取名返回前端 例：id='uid'
        :return: {'uid': 1, 'name': 'jack', ...}
        """
        attr_dict = {}
        exclude = exclude or []
        for field in self._meta.fields.values():
            name = field.column_name
            if name in exclude:
                continue
            value = getattr(self, name)
            if isinstance(value, datetime):
                value = value.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(value, date):
                value = value.strftime("%Y-%m-%d")
            attr_dict[name] = value
        for name, alias_name in key_alias.items():
            value = attr_dict.pop(name, None)
            attr_dict[alias_name] = value

        return attr_dict

    def to_dict_with_picture(self, *keys, **key_alias):
        return self.to_dict(*keys, **key_alias)

    def dump_to_dict(self):
        d = {}
        keys = self._meta.fields.keys()
        for k in keys:
            value = getattr(self, k)
            if isinstance(value, datetime):
                d[k] = value.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(value, BaseModel):
                d[self._meta.fields[k].column_name] = value.get_id()
            else:
                d[k] = value
        return d

    @classmethod
    def get_ref_table(cls):
        for k, v in cls._meta.fields.items():
            if isinstance(v, ForeignKeyField):
                yield v.rel_model._meta.table_name

    @classmethod
    def from_dict(cls, kwargs):
        obj = cls()
        for k, v in kwargs.items():
            if hasattr(obj, k):
                setattr(obj, k, v)
        return obj


    @classmethod
    def remove_fields_not_in_model(cls, line, fields_in_model):
        # print('-------', line)
        fields_need_pop = line.keys() - fields_in_model
        line.pop(cls._meta.primary_key.column_name, 0)
        for k in fields_need_pop:
            line.pop(k)

    @classmethod
    def create(cls, **query):
        cls.fill_common_fields(query)
        return super(BaseModel, cls).create(**query)

    @classmethod
    def create_with_safe(cls, **create):
        create.pop(cls._meta.primary_key.column_name, None)
        fields_in_model = [v.column_name for k, v in cls._meta.fields.items()]
        cls.remove_fields_not_in_model(create, fields_in_model)
        return cls.create(**create)

    @classmethod
    def insert_many_with_session(cls, rows, fields=None, add_session=True):
        fields_in_model = [v.column_name for k, v in cls._meta.fields.items()]
        for item in rows:
            if add_session:
                cls.fill_common_fields(item)
            cls.remove_fields_not_in_model(item, fields_in_model)
        # print(rows)
        return super(BaseModel, cls).insert_many(rows, fields)

    @classmethod
    def gc_update(cls, __data=None, **update):
        fields_in_model = [v.column_name for k, v in cls._meta.fields.items()]
        cls.remove_fields_not_in_model(__data, fields_in_model)
        return super(BaseModel, cls).update(__data, **update)

    @classmethod
    def gc_insert_many(cls, rows, fields=None, add_session=True):
        fields_in_model = [v.column_name for k, v in cls._meta.fields.items()]
        for item in rows:
            cls.remove_fields_not_in_model(item, fields_in_model)
        return super(BaseModel, cls).insert_many(rows, fields)

    @classmethod
    def base_create(cls, **query):
        return super(BaseModel, cls).create(**query)

    @classmethod
    def add(cls, d):
        print("IN add", cls, d)
        cls.fill_common_fields(d)
        return cls.insert(d).execute()

    @classmethod
    def add_all(cls, l):
        for one in l:
            cls.add(one)

    @classmethod
    def rm(cls, **kwargs):
        query = cls.delete()
        filters = []
        for k, v in kwargs.items():
            if v is None:
                continue
            attr = getattr(cls, k, None)
            if attr:
                filters.append(attr == v)
        if filters:
            filters = tuple(filters)
            query = query.where(*filters)
        query.execute()

    @classmethod
    def edit(cls, data, **kwargs):
        query = cls.update(data)
        filters = []
        for k, v in kwargs.items():
            if v is None:
                continue
            attr = getattr(cls, k, None)
            if attr:
                filters.append(attr == v)
        if not filters:
            return
        filters = tuple(filters)
        query = query.where(*filters)
        return query.execute()

    @classmethod
    def get_count(cls, **kwargs):
        return cls.get_all(**kwargs).count()
        # query = cls.select()
        # filters = []
        # for k, v in kwargs.items():
        #     if v is None:
        #         continue
        #     attr = getattr(cls, k, None)
        #     if attr:
        #         filters.append(attr == v)
        # if filters:
        #     filters = tuple(filters)
        #     query = query.where(*filters)
        # return query.count()

    @staticmethod
    def page(query, limit, page):
        limit = int(limit) if limit else 0
        page = int(page) if page else 1
        offset = (page - 1) * limit
        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)
        return query

    @classmethod
    def desc(cls, query, desc):
        attr = getattr(cls, desc, None)
        if attr:
            query = query.order_by(attr.desc())
        return query

    @classmethod
    def get_all(cls, limit=0, offset=0, order_by=None, **kwargs):
        q = QueryHelper(cls)
        q.where(**kwargs)
        if order_by:
            q.order_by(order_by)
        if limit:
            q.limit(limit)
        if offset:
            q.offset(offset)
        return q
        # query = cls.select()
        # filters = []
        # for k, v in kwargs.items():
        #     if v is None:
        #         continue
        #     attr = getattr(cls, k, None)
        #     if attr is not None:
        #         if isinstance(v, list):
        #             filters.append(attr.in_(v))                    # in
        #         elif isinstance(v, tuple):
        #             if len(v) == 1 and v[0] is not None:
        #                 filters.append(attr ** ("%%%s%%" % v[0]))  # ilike 单个元素的tuple代表模糊查询
        #             elif len(v) == 2:
        #                 pass
        #         else:
        #             filters.append(attr == v)
        # if filters:
        #     filters = tuple(filters)
        #     query = query.where(*filters)
        # if offset:
        #     query = query.offset(offset)
        # if limit:
        #     query = query.limit(limit)
        # return query

    @classmethod
    def get_all_active(cls, limit=0, offset=0, **kwargs):
        kwargs['code_status'] = 1
        return cls.get_all(limit, offset, **kwargs)

    @classmethod
    def get_one(cls, **kwargs):
        query = cls.select()
        filters = []
        for k, v in kwargs.items():
            if v is None:
                continue
            attr = getattr(cls, k, None)
            if attr:
                filters.append(attr == v)
        if filters:
            filters = tuple(filters)
            query = query.where(*filters)
        one = query.first()
        if one:
            return one
        else:
            return SqlNone(cls, kwargs)

    @classmethod
    def get_by_id(cls, pk):
        try:
            return super(BaseModel, cls).get_by_id(pk)
        except DoesNotExist as e:
            return SqlNone(cls, pk)

    @classmethod
    def get_by_unique_id(cls, uk):
        try:
            return cls.select().where(cls.unique_id == uk).first()
        except DataError as e:
            return SqlNone(cls, uk)



    def get_child_lines(self, id_value, main_model, main_rel_column, child_model, child_rel_column, main_sort="id",
                        child_sort="id"):
        datalist = []
        main_ids = []
        main_querys = (main_model.select()
                       .where(getattr(main_model, main_rel_column) == id_value)
                       .order_by(getattr(main_model, main_sort).asc()))
        if main_querys:
            for main_query in main_querys:
                main_ids.append(main_query.id)
                datalist.append(main_query.to_dict())
            line_id_dict = defaultdict(list)
            child_querys = (child_model.select()
                            .where(getattr(child_model, child_rel_column) << main_ids)
                            .order_by(getattr(child_model, child_rel_column), getattr(child_model, child_sort).asc()))
            if child_querys:
                for child_query in child_querys:
                    cur_data = child_query.to_dict()
                    line_id_dict[cur_data[child_rel_column]].append(cur_data)

            for item in datalist:
                item["lines"] = line_id_dict[item["id"]]
        return datalist

    @classmethod
    def create_table(cls, safe=True, **options):
        super(BaseModel, cls).create_table(safe=True, **options)
        cls.create_comments()

    @classmethod
    def create_comments(cls):
        table_name = cls._meta.table_name
        for field_name, field in cls._meta.columns.items():
            if getattr(field, "help_text", None) and field.help_text:
                help_text = field.help_text
                sql = """COMMENT ON COLUMN {table_name}.{field_name} is '{help_text}'""" \
                    .format(table_name=table_name, field_name=field_name, help_text=help_text)
                cls._meta.database.execute_sql(sql)

    @classmethod
    def qty_update_or_create(cls, **kwargs):
        """
        部分字段查询数据，查询出多条选择第一条进行数量更新，未查询到结果，创建新数据，没有主键的模型，不适用此方法
        :param defaults: 对应需要更新的数据
        :param qty_defaults: 对应在原数量基础上需要加的数量
        :return: instance, bool
        example:
        User.qty_update_or_create(defaults={'school':'家里蹲大学'},qty_defaults={'money':100},name='jack',sex='male')
        """

        qty_defaults = kwargs.pop('qty_defaults', {})
        defaults = kwargs.pop('defaults', {})
        query = cls.select()
        fields_in_model = [v.column_name for k, v in cls._meta.fields.items()]
        cls.remove_fields_not_in_model(qty_defaults, fields_in_model)
        cls.remove_fields_not_in_model(defaults, fields_in_model)
        # cls.remove_fields_not_in_model(kwargs, fields_in_model)
        for field, value in kwargs.items():
            query = query.where(getattr(cls, field) == value)
        instance = query.first()
        if instance:
            _where = (getattr(cls, cls._meta.primary_key.safe_name) == instance.get_id())
            qty_update = {field: qty + getattr(cls, field)
                          for field, qty in qty_defaults.items()}
            if qty_update or defaults:
                update_models = cls.update(**{**qty_update, **defaults}).where(_where).returning(cls).execute()
                return update_models[0], False
            else:
                return instance, False
        kwargs.update(defaults)
        kwargs.update(qty_defaults)
        return cls.create(**kwargs), True

    @classmethod
    def bulk_update_batch(cls, update_list, update_columns, where_columns, batch_size=None):
        """ !!! 不支持update_columns和where_columns字段名有重复的情况，有此需求，请使用ValuesList更新  ！！！
        @param update_list: 更新的字典列表
        @param update_columns: 需要更新的字段,['name']
        @param where_columns: 作为条件的字段,['id']
        @param batch_size: 批次更新的数量, 默认一次性更新
        @return: 更新的条数
        count_ = MyCommonCity.bulk_update_batch(update_list=[{'id':1,'name':"北京"}, {'id':5,'name':"上海"}],
                                                update_columns=['name'], where_columns=['id'], batch_size=200)
        转换的sql：
            UPDATE my_common_city SET name = t1.name
            FROM ( VALUES( 1, '北京' ),( 5, '上海', )) AS t1( id, name )
            WHERE my_common_city.id = t1.id;
        """
        if not all([update_list, update_columns, where_columns]):
            return 0
        if set(update_columns) & set(where_columns):
            raise Exception('更新字段和条件字段不能重复')
        base_field_type = cls.field_type_dict()
        table_fields = {}
        for k, v in cls._meta.fields.items():
            table_fields[v.column_name] = base_field_type[getattr(cls, k).field_type]
            table_fields[k] = base_field_type[getattr(cls, k).field_type]
        db_columns = [column for column in (update_columns + where_columns)
                      if column in table_fields]
        values = [[update_item[column] for column in db_columns]
                  for update_item in update_list]
        batch_size = batch_size or len(values)
        n_update = 0
        for i in range(0, len(values), batch_size):
            values_list = ValuesList(values[i: i + batch_size], columns=db_columns)
            update_dict = {column: getattr(values_list.c, column).cast(table_fields[column])
                           for column in update_columns}
            conditions = [getattr(cls, column) == getattr(values_list.c, column).cast(table_fields[column])
                          for column in where_columns]
            base_update = cls.update(**update_dict).from_(values_list).where(*conditions)
            n_update += (base_update.execute())

        return n_update

    @classmethod
    def field_type_dict(cls):
        base_field_type = deepcopy(FIELD)
        postgres_field_type = {'BLOB': 'BYTEA',
                               'BOOL': 'BOOLEAN',
                               'DATETIME': 'TIMESTAMP',
                               'DECIMAL': 'NUMERIC',
                               'DOUBLE': 'DOUBLE PRECISION',
                               'UUID': 'UUID',
                               'UUIDB': 'BYTEA'}
        base_field_type.update(postgres_field_type)

        return base_field_type

    @classmethod
    def upsert(cls, rows: List[Dict[str, Any]], conflict_target, update=None, add_session=True):
        """

        :param rows:
        :param conflict_target:
        :param update:
        :param add_session:
        :return:
        """
        if not rows:
            return

        # add session data
        fields_in_model = [v.column_name for k, v in cls._meta.fields.items()]
        for item in rows:
            if add_session:
                cls.fill_common_fields(item)
            cls.remove_fields_not_in_model(item, fields_in_model)

        fields = [k for k in rows[0]]

        # add default data
        a = cls.meta_info()
        defaults = a.defaults
        default_values = {}
        for k, v in defaults.items():
            field = k.safe_name
            if isinstance(v, SQL):
                value = SQL(v.sql)
            elif callable(v):
                value = v()
            else:
                value = v

            if field not in fields:
                fields.append(field)
                default_values[field] = value

        value_list = []

        for row in rows:
            copy_row = row.copy()
            for k, v in copy_row.items():
                if isinstance(v, (list, tuple, set)):
                    # cast type integer to character varying[]
                    copy_row[k] = '{' + ','.join(f'"{i}"' if isinstance(i, str) else str(i) for i in v) + '}'
            for k, v in default_values.items():
                if not copy_row.get(k):
                    copy_row[k] = v
            value_list.append(tuple(copy_row.values()))
        values = ValuesList(value_list, columns=fields)

        wheres = []
        on = (1 == 1)
        for conflict in conflict_target:
            c = getattr(cls, conflict) == getattr(values.c, conflict)
            wheres.append(c)
            on &= c

        # update or select with constraint condition
        if update:
            for k, v in update.items():
                model_field = getattr(cls, k)
                cast = cls.field_type_dict()[model_field.field_type]
                if isinstance(model_field, ArrayField):
                    cast += '[]'
                if isinstance(v, NamespaceAttribute):
                    update[k] = getattr(values.c, v.__dict__['_attribute']).cast(cast)
                elif isinstance(v, Expression) and isinstance(v.rhs, NamespaceAttribute):
                    v.rhs = getattr(values.c, v.rhs.__dict__['_attribute']).cast(cast)
            old_queryset = cls.update(**update).from_(values).where(*wheres).returning(cls).execute()
        else:
            old_queryset = cls.select(cls).join(values, on=on)

        old_value_list = [[getattr(query, conflict) for conflict in conflict_target] for query in old_queryset]
        if old_value_list:
            old_values_cte = ValuesList(old_value_list).cte('old_values', columns=conflict_target)

            # constraint condition
            insert_where = [
                getattr(values.c, conflict) == getattr(old_values_cte.c, conflict) for conflict in conflict_target
            ]
            # cast field to specify type
            select = []
            for field in fields:
                model_field = getattr(cls, field)
                cast = cls.field_type_dict()[model_field.field_type]
                if isinstance(model_field, ArrayField):
                    cast += '[]'
                _field = getattr(values.c, field).cast(cast)
                select.append(_field)

            returning_queryset = (cls
                                  .insert_from((values
                                                .select(*select)
                                                .where(~fn.EXISTS(old_values_cte
                                                                  .select()
                                                                  .where(*insert_where)))),
                                               fields)
                                  .returning(cls)
                                  .with_cte(old_values_cte)
                                  .execute())
        else:
            returning_queryset = cls.insert_many_with_session(rows).returning(cls).execute()

        return [*list(returning_queryset), *list(old_queryset)]


    @classmethod
    def get_id_map_column(cls, column, where_domain=None):
        queries = cls.select(cls.id, getattr(cls, column)).order_by(cls.id)
        if where_domain:
            queries = queries.where(*where_domain)
        return {query.id: getattr(query, column) for query in queries}

    @classmethod
    def get_id_map_columns(cls, columns, where_domain=None):
        select_domain = [getattr(cls, column) for column in columns]
        queries = cls.select(cls.id, *select_domain).order_by(cls.id)
        if where_domain:
            queries = queries.where(*where_domain)
        res = {}
        for query in queries:
            _di = {column: getattr(query, column) for column in columns}
            res[query.id] = _di
        return res

    @classmethod
    def get_max_id(cls):
        """
        returns max(id) of the table
        """
        query_max_id = cls.select(cls.id).order_by(-cls.id).first()
        max_id = query_max_id.id if query_max_id else 0
        return max_id
