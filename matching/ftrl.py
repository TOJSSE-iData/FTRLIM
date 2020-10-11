# coding=utf-8

import numpy as np
import json


class LR(object):

    @staticmethod
    def fn(w, x):
        return 2.0 / (1.0 + np.exp(-w.dot(x))) - 1

    @staticmethod
    def loss(y, y_hat):
        return np.sum(np.nan_to_num(-y * np.log(y_hat) - (1 - y) * np.log(1 - y_hat)))

    @staticmethod
    def grad(y, y_hat, x):
        return (y_hat - y) * x


class FTRL(object):

    @staticmethod
    def load_model(path):
        with open(path) as reader:
            params = json.load(reader)
        mdl = FTRL(
            dim=params['dim'],
            l1=params['l1'],
            l2=params['l2'],
            alpha=params['alpha'],
            beta=params['beta'],
            max_iter=params['max_iter'],
            eta=params['eta'],
            epochs=params['epochs']
        )
        mdl.z = np.asarray(params['z'])
        mdl.n = np.asarray(params['n'])
        mdl.w = np.asarray(params['w'])
        mdl.trained = params['trained']
        return mdl

    def __init__(self, dim, l1, l2, alpha, beta, max_iter=10000, eta=0.01, epochs=100):
        self.dim = dim
        self.decision_func = LR
        self.z = np.zeros(dim)
        self.n = np.zeros(dim)
        self.w = np.zeros(dim)
        self.l1 = l1
        self.l2 = l2
        self.alpha = alpha
        self.beta = beta
        self.max_iter = max_iter
        self.eta = eta
        self.epochs = epochs
        self.trained = False

    def predict(self, x):
        if self.trained:
            return self.decision_func.fn(self.w, x)
        else:
            return sum(x) / len(x)

    def update(self, x, y):
        self.w = np.array([0 if np.abs(self.z[i]) <= self.l1 else (np.sign(self.z[i]) * self.l1 - self.z[i]) / (
                self.l2 + (self.beta + np.sqrt(self.n[i])) / self.alpha) for i in range(self.dim)])
        y_hat = self.predict(x)
        g = self.decision_func.grad(y, y_hat, x)
        sigma = (np.sqrt(self.n + g * g) - np.sqrt(self.n)) / self.alpha
        self.z += g - sigma * self.w
        self.n += g * g
        self.trained = True
        return self.decision_func.loss(y, y_hat)

    def train(self, train_set):
        itr = 0
        n = 0
        while True:
            for x, y in train_set:
                loss = self.update(x, y)
                if loss < self.eta:
                    itr += 1
                else:
                    itr = 0
                if itr >= self.epochs:
                    print("loss have less than", self.eta, " continuously for ", itr, "iterations")
                    return
                n += 1
                if n >= self.max_iter:
                    print("reach max iteration", self.max_iter)
                    return

    def save(self, path):
        params = {
            'dim': self.dim,
            'l1': self.l1,
            'l2': self.l2,
            'alpha': self.alpha,
            'beta': self.beta,
            'max_iter': self.max_iter,
            'eta': self.eta,
            'epochs': self.epochs,
            'trained': self.trained,
            'z': self.z.tolist(),
            'n': self.n.tolist(),
            'w': self.w.tolist(),
        }
        with open(path, 'w') as writer:
            json.dump(params, writer)


class Corpus(object):

    def __init__(self, dim, data):
        self.dim = dim
        self.data = data

    def __iter__(self):
        for arr in self.data:
            if len(arr) == (self.dim + 1):
                yield np.array([float(x) for x in arr[0:self.dim]]), float(arr[self.dim])
