import os
import datetime
import pandas as pd
import xml.etree.ElementTree as ET

class main():
    def __init__(self, file_path):
        self.file_path = file_path
        self.type_dic = {'HKQuantityTypeIdentifierHeight': 'Height',
                           'HKQuantityTypeIdentifierBodyMass': 'Body_Mass',
                           'HKQuantityTypeIdentifierStepCount': 'Step_Count',
                           'HKQuantityTypeIdentifierDistanceWalkingRunning': 'Distance_Walking_Running',
                           'HKQuantityTypeIdentifierFlightsClimbed': 'Flights_Climbed',
                           'HKQuantityTypeIdentifierHeadphoneAudioExposure': 'Headphone_Audio_Exposure',
                           'HKQuantityTypeIdentifierWalkingDoubleSupportPercentage': 'Walking_Double_Support_Percentage',
                           'HKQuantityTypeIdentifierWalkingSpeed': 'Walking_Speed',
                           'HKQuantityTypeIdentifierWalkingStepLength': 'Walking_Step_Length',
                           'HKQuantityTypeIdentifierWalkingAsymmetryPercentage': 'Walking_Asymmetry_Percentage',
                           'HKCategoryTypeIdentifierSleepAnalysis': 'Sleep_Analysis',
                           'HKCategoryTypeIdentifierHeadphoneAudioExposureEvent': 'Headphone_Audio_Exposure_Event'}
        self.unit_dict = {'Headphone_Audio_Exposure': 'dBASPL',
                            'Walking_Double_Support_Percentage': '%',
                            'Walking_Speed': 'km/hr',
                            'Walking_Step_Length': 'cm',
                            'Distance_Walking_Running': 'km',
                            'Flights_Climbed': 'count',
                            'Step_Count': 'count'}

    def xml_to_df(self, file_path:str):
        tree = ET.parse(self.file_path)
        root = tree.getroot()

        ignore_index = False
        for record_item in root.iter('Record'):
            record = record_item.attrib
            if ignore_index == False:
                health_df = pd.DataFrame([record])
                ignore_index = True
                continue
            health_df = health_df.append([record], ignore_index=ignore_index)
        return health_df

    def __datetime_formating(self, time):
        datetime_formated = datetime.datetime.strptime(time[:-6], '%Y-%m-%d %X')
        datetime_formated = datetime_formated.date()
        return datetime_formated
    
    def __preprocessing(self, df):
        ## Convert date features to datetime type 
        df.creationDate = df.creationDate.apply(self.__datetime_formating)
        df.startDate = df.startDate.apply(self.__datetime_formating)
        df.endDate = df.endDate.apply(self.__datetime_formating)
        
        ## Formatting type features
        df.type = df.type.apply(lambda x: self.type_dic[x])
        
        return df
    
    def get_dataframe(self):
        df = self.xml_to_df(self.file_path)
        df = self.__preprocessing(df)
        
        to_group_by_sum = df[df['type'].isin(['Step_Count',
                                                     'Distance_Walking_Running',
                                                     'Flights_Climbed'])]
        to_group_by_mean = df[df['type'].isin(['Headphone_Audio_Exposure',
                                                'Walking_Double_Support_Percentage',
                                                  'Walking_Speed',
                                                    'Walking_Step_Length'])]
        
        to_group_by_sum.value = to_group_by_sum.value.astype(float)
        to_group_by_mean.value = to_group_by_mean.value.astype(float)
        
        sum_grp = to_group_by_sum[['type', 'creationDate', 'value']].groupby(by=['type', 'creationDate']).sum()
        mean_grp = to_group_by_mean[['type', 'creationDate', 'value']].groupby(by=['type', 'creationDate']).mean()
        
        frames = [mean_grp, sum_grp]
        result_df = pd.concat(frames)
        result_df.reset_index(inplace=True)
        
        result_df['unit'] = result_df.type.apply(lambda type: self.unit_dict[type])
        
        return result_df
        

