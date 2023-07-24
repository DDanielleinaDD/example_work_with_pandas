import json
import pandas as pd


class WarehouseAnalysis:
    def __init__(self, data_file):
        self.data_file = data_file
        self.data = None
        self.warehouse_df = None
        self.products_info = None
        self.warehouse_result_df = None
        self.sorted_warehouse_result_df = None

    def read_data(self):
        '''Считываем данные из json файла.'''
        with open(self.data_file, 'r', encoding='utf-8') as file:
            self.data = json.load(file)

    def calculate_tariffs(self):
        '''Получаем тарифы по каждому складу.'''
        df = pd.json_normalize(self.data)
        warehouse_names = df['warehouse_name']
        highway_costs = df['highway_cost']
        self.warehouse_df = pd.DataFrame({'warehouse_name': warehouse_names,
                                          'highway_cost': highway_costs})
        self.warehouse_df.to_excel('1-tariffs.xlsx', index=False)  # Сохраняем результат в файл tariffs.xlsx

    def calculate_product_summary(self):
        '''Считаем суммарное количество, суммарный доход, расход и прибыль
            для каждого товара.'''
        products_info = pd.json_normalize(
            self.data,
            record_path='products',
            meta=['order_id', 'warehouse_name', 'highway_cost'])

        products_info['expenses'] = (products_info['quantity']
                                     * products_info['highway_cost'])

        products_summary = products_info.groupby('product').agg({
            'quantity': 'sum',
            'price': 'sum',
            'expenses': 'sum'
        }).reset_index()

        products_summary['income'] = (products_summary['quantity']
                                      * products_summary['price'])
        products_summary['profit'] = (products_summary['income']
                                      - abs(products_summary['expenses']))
        products_summary.to_excel('2-products_summary.xlsx', index=False)  # Сохраняем результат в файл products_summary.xlsx

        self.products_info = products_info  # Сохраняем результат в атрибут класса

    def calculate_orders_summary(self):
        '''Считаем прибыль каждого заказа.'''
        self.products_info['income'] = (self.products_info['quantity']
                                        * self.products_info['price'])

        orders_summary = self.products_info.groupby('order_id').agg({
            'income': 'sum',
            'expenses': 'sum'
        }).reset_index()

        orders_summary['order_profit'] = (orders_summary['income']
                                          - abs(orders_summary['expenses']))
        orders_summary.to_excel('3-orders_summary.xlsx',
                                columns=('order_id', 'order_profit'),
                                index=False)  # Сохраняем результат в файл orders_summary.xlsx

    def calculate_warehouse_summary(self):
        '''Считаем процент прибыли каждого товара к прибыли скалада, откуда заказан товар.'''
        warehouse_result = self.products_info.groupby(['warehouse_name', 'product']).agg({
                        'quantity': 'sum',
                        'expenses': 'sum',
                        'income': 'sum'
                    }).reset_index()
        warehouse_result['profit'] = (warehouse_result['income']
                                      - abs(warehouse_result['expenses']))

        warehouse_result_all = warehouse_result.groupby('warehouse_name').agg({
            'profit': 'sum'
        }).reset_index()

        total_profit_per_warehouse = warehouse_result_all.rename(columns={'profit': 'total_profit'})

        self.warehouse_result_df = warehouse_result.merge(total_profit_per_warehouse, on='warehouse_name', how='left')

        self.warehouse_result_df['percent_profit_product_of_warehouse'] = (self.warehouse_result_df['profit'] / self.warehouse_result_df['total_profit']) * 100

        self.warehouse_result_df.to_excel('4-warehouse_result.xlsx', columns=('warehouse_name', 'product',
                                                                            'quantity', 'total_profit',
                                                                            'percent_profit_product_of_warehouse'), index=False)  # Сохраняем результат в файл warehouse_result.xlsx

    def calculate_accumulated_percent(self):
        '''Считаем накопительный процент для каждого склада.'''
        self.sorted_warehouse_result_df = self.warehouse_result_df.sort_values(by=['warehouse_name',
                                                                                   'percent_profit_product_of_warehouse'], ascending=[True, False]).copy()
        self.sorted_warehouse_result_df['accumulated_percent_profit_product_of_warehouse'] = 0
        current_warehouse = None
        current_accumulated_percent = 0

        for index, row in self.sorted_warehouse_result_df.iterrows():
            if current_warehouse is None or current_warehouse != row['warehouse_name']:
                current_warehouse = row['warehouse_name']
                current_accumulated_percent = 0
            current_accumulated_percent += row['percent_profit_product_of_warehouse']
            self.sorted_warehouse_result_df.at[index, 'accumulated_percent_profit_product_of_warehouse'] = current_accumulated_percent

    def get_category(self, percent):
        '''Рассчет категории для товаров со склада.'''
        if percent <= 70:
            return 'A'
        elif percent <= 90:
            return 'B'
        return 'C'

    def add_category_column(self):
        '''Добавляем категории к товарам в соотвествии с ТЗ.'''
        self.sorted_warehouse_result_df['category'] = self.sorted_warehouse_result_df['accumulated_percent_profit_product_of_warehouse'].apply(self.get_category)

    def export_accumulated_percent(self):
        '''Сохраняем итоговый результат в файл accumulated_percent.xlsx.'''
        self.sorted_warehouse_result_df.to_excel('5-accumulated_percent.xlsx',
                                                 columns=('warehouse_name', 'product',
                                                          'quantity', 'total_profit',
                                                          'percent_profit_product_of_warehouse',
                                                          'accumulated_percent_profit_product_of_warehouse', 'category'), index=False)

    def run_analysis(self):
        self.read_data()
        self.calculate_tariffs()
        self.calculate_product_summary()
        self.calculate_orders_summary()
        self.calculate_warehouse_summary()
        self.calculate_accumulated_percent()
        self.add_category_column()
        self.export_accumulated_percent()


if __name__ == '__main__':
    # Создаем объект класса и запускаем анализ
    analysis = WarehouseAnalysis('trial_task.json')
    analysis.run_analysis()
