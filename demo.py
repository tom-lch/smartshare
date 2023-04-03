import torch
import torch.nn as nn
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# 设置随机种子以便复现结果
torch.manual_seed(1)

# 加载数据
data = pd.read_csv('stock_prices.csv', usecols=[1,2,3,4])
plt.plot(data)
plt.show()

# 归一化数据
from sklearn.preprocessing import MinMaxScaler

scaler = MinMaxScaler(feature_range=(-1, 1))
data = scaler.fit_transform(data)

# 分割数据集
train_size = int(len(data) * 0.7)
test_size = len(data) - train_size
train_data, test_data = data[0:train_size,:], data[train_size:len(data),:]

# 定义函数将数据集转换为带有时间步长的数据集
def create_dataset(dataset, look_back=1):
    dataX, dataY = [], []
    for i in range(len(dataset)-look_back-1):
        a = dataset[i:(i+look_back), :]
        dataX.append(a)
        dataY.append(dataset[i + look_back, :])
    return np.array(dataX), np.array(dataY)

# 使用前60天的股票价格预测下一天的价格
look_back = 60
trainX, trainY = create_dataset(train_data, look_back)
testX, testY = create_dataset(test_data, look_back)

# 将数据集从numpy数组转换为张量
trainX = torch.from_numpy(trainX).type(torch.Tensor)
trainY = torch.from_numpy(trainY).type(torch.Tensor)
testX = torch.from_numpy(testX).type(torch.Tensor)
testY = torch.from_numpy(testY).type(torch.Tensor)

# 定义LSTM模型
class LSTM(nn.Module):
    def __init__(self, input_size=4, hidden_size=20, output_size=4):
        super().__init__()
        self.hidden_size = hidden_size
        self.lstm = nn.LSTM(input_size, hidden_size)
        self.linear = nn.Linear(hidden_size, output_size)

    def forward(self, input):
        lstm_out, _ = self.lstm(input.view(len(input), 1, -1))
        y_pred = self.linear(lstm_out.view(len(input), -1))
        return y_pred[-1]

# 实例化模型并定义优化器和损失函数
model = LSTM()
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# 训练模型
num_epochs = 1000
train_losses = []
for epoch in range(num_epochs):
    optimizer.zero_grad()
    outputs = model(trainX)
    loss = criterion(outputs, trainY)
    train_losses.append(loss)
    loss.backward()
    optimizer.step()
    if epoch % 100 == 0:
        print('Epoch [{}/{}], Loss: {:.4f}'.format(epoch+1, num_epochs, loss.item()))

# 使用测试集验证模型精度
model.eval()
test_predict = model(testX)

# 将张量转换为numpy数组
test_predict = test_predict.detach().numpy()
testY = testY.numpy()

# 将预测结果反归一化
test_predict = scaler.inverse_transform(test_predict)
testY = scaler.inverse_transform(testY)

# 计算均方根误差（RMSE）
from sklearn.metrics import mean_squared_error

rmse = np.sqrt(mean_squared_error(testY, test_predict))
print('Test RMSE: {:.2f}'.format(rmse))

# 绘制预测结果
plt.plot(testY[:,0], label='Actual Prices')
plt.plot(test_predict[:,0], label='Predicted Prices')
plt.legend()
plt.show()
