# smartshare
项目结构划分
## addons 
addons 下面的每一个模块是一个独立的计算模型，用于分析预测，建议每一个都定义自己策略逻辑
## base 
base 存放项目启动必须的逻辑
## strategies
存放基础策略，方法简单
## models
深度学习模型引用路径
## utils

utils 存放开发中封装的便利工具

conda  install --yes --file requirements.txt

pip install tushare=1.2.89
pip install requests=2.23.2