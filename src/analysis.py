import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import RFE
from sklearn.linear_model import Lasso
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt

# Load your dataset
df = pd.read_csv(r'C:\github\P.910\Teleport_a_11_22_2023\results\Teleport_a_11_22_2023_raw_data_pivot.csv')

df = df[['url', 'appropriate', 'creepy', 'formal', 'realistic', 'trust', 'comfortableusing', 'comfortableinteracting']]

# avg on clip level
df = df.groupby(['url']).mean()

plt.figure(figsize=(5, 5))
plt.plot(df['realistic'], df['comfortableusing'], 'g.')
plt.xlabel('realistic')
plt.ylabel('comfortableusing')
plt.xlim(0, 6)
plt.ylim(0, 6)
plt.show()


plt.figure(figsize=(5, 5))
plt.plot(df['realistic'], df['comfortableinteracting'], 'g.')
plt.xlabel('realistic')
plt.ylabel('comfortableinteracting')
plt.xlim(0, 6)
plt.ylim(0, 6)
plt.show()



# Assuming you have columns 'X1', 'X2', ..., 'Y' in your dataset
X = df[['appropriate', 'creepy', 'formal', 'realistic',
        'trust']]  # Include all the columns you want to use as independent variables

dependent_variables = ['comfortableusing', 'comfortableinteracting']


def fit_and_evaluate(model, X_train, y_train, X_test, y_test):
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error: {np.round(mse, 3)}')
    print('Predictions of test set:', np.round(np.mean(y_pred), 2), 'actual value', np.round(np.mean(y_test), 2))
    # Assuming you have a DataFrame 'df' with columns 'X' and 'Y'
    correlation, p_value = pearsonr(y_pred, y_test)
    print(f'Pearson Correlation Coefficient: {np.round(correlation, 2)}', ' ', f'P-value: {np.round(p_value, 5)}')


for dependent_variable in dependent_variables:
    print('Dependent Variable:', dependent_variable)
    print("Linear Regression:")
    y = df[dependent_variable]

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.2, random_state=42)
    X_train =X
    X_test = X
    y_train = y
    y_test = y

    model = LinearRegression()
    fit_and_evaluate(model, X_train, y_train, X_test, y_test)

    feature_importances = pd.DataFrame({'Feature': X.columns, 'Coefficient': model.coef_})
    feature_importances['Abs_Coefficient'] = feature_importances['Coefficient'].abs()
    feature_importances = feature_importances.sort_values(by='Abs_Coefficient', ascending=False)
    print(feature_importances)
    print('==================' * 2)

    print('Recursive Feature Elimination:')
    model = LinearRegression()
    rfe = RFE(model, n_features_to_select=1)
    fit = rfe.fit(X_train, y_train)

    y_pred = rfe.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f'Mean Squared Error: {mse}')

    feature_ranking_rfe = pd.DataFrame({'Feature': X.columns, 'Ranking': fit.ranking_})
    feature_ranking_rfe = feature_ranking_rfe.sort_values(by='Ranking')
    print(feature_ranking_rfe)
    print('==================' * 2)

    print('Random Forest Regressor:')
    model_rf = RandomForestRegressor()
    fit_and_evaluate(model_rf, X_train, y_train, X_test, y_test)

    feature_importances_rf = pd.DataFrame({'Feature': X.columns, 'Importance': model_rf.feature_importances_})
    feature_importances_rf = feature_importances_rf.sort_values(by='Importance', ascending=False)
    print(feature_importances_rf)
    print('==================' * 2)

    print('Lasso Regression:')
    model_lasso = Lasso(alpha=0.1)  # Adjust alpha for regularization strength
    fit_and_evaluate(model_lasso, X_train, y_train, X_test, y_test)

    feature_coefficients_lasso = pd.DataFrame({'Feature': X.columns, 'Coefficient': model_lasso.coef_})
    feature_coefficients_lasso = feature_coefficients_lasso[feature_coefficients_lasso['Coefficient'] != 0]
    print(feature_coefficients_lasso)



