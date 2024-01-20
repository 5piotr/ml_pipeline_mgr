from sklearn.cluster import KMeans
import lib

query = '''
    select * from apt_details
    where date = (select max(date) from apt_details)
    '''

data = lib.get_df_from_mysql(query)

X = data.loc[:,['localization_y','localization_x']].values
kmeans = KMeans(n_clusters = 600, n_init='auto', random_state = 0).fit(X)
data['cluster'] = kmeans.labels_

lib.save_pkl(data, 'train_data/apt_details_cls.pkl')
lib.save_pkl(kmeans, '../models/temp/kmeans.pkl')
