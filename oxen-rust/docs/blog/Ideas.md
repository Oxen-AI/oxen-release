# Blog Ideas (try to keep to 1 min)

* Machine Inference != Machine Learning
    - Trend of using pretrained models
    - Many advantages
        - Implement into your product
        - Evaluate baseline accuracy, throughput, latency
    - Many issues
        - Cannot improve upon
        - Failure in the cases in the wild, what do you do?
        - May have been trained with improper license
    - Conclusion
        - We are working helping companies with this problem, contact us for access

* Start with the Data
    - You should have your retraining pipeline ready to go when you deploy your app to prod
        - you do this for code, many do not for ML
    - Ideally you start with a model and dataset you can train on that data
    - Where do you get the data?
        - Cross link to tweets or other posts
        - Share with person that tweeted
    - How do you update the data?
    - What do you do if a model checkpoint is producing errors?
    - How do you even analyze those errors?
    - Conclusion
        - Create an oxen account to manage your data properly

* Your Current Eval Dataset sucks
    - Your current evaluation dataset may not be that great
        - Incorrectly tagged
        - Small images
        - Not representitive of actual use case
    - You should be finding error cases in the wild, iterating on, and fixing
    - Always a moving target
    - Need ability to go back to versions
    - Need ability to visualize false positives, true negatives, etc
        - Find me all the images where there was a wrist but I didn't tag
    - Conclusion
        - Need oxen to help manage evaluation dataset

* Human Performance Analysis
    - 4 key ML problems:
        - Classification (is there even a human in this image?)
        - Bounding box detection
        - Person Keypoints
        - Action classification based on keypoints
    - Conclusion
        - Link to 4 Oxen datasets


* Computer Vision Outcomes, Business Cases

    These are to spark the imagination, ping us with other ideas

        - So you want to start a dog park, AI beauty pagent, etc?
        - Point to other peoples works

    v1) 1 min bites of imagination
    v2) We link to a dataset in Oxen
    v3) We show training / eval on that dataset


    - Human Performance Analysis
        - Person Classification
        - Person Bounding Box
        - Person Keypoints
        - Person Action Classification
        - Person Action Prediction

    - Animal Identification, and Analysis
        - Tagging in the wild
        - Facial Recognition

    - Receipt Scanning
        - Image Classification
        - Image Bounding Box
        - Image OCR

    - Satellite Imagery
        - https://github.com/robmarkcole/satellite-image-deep-learning
        - Land Use/Cover
        - Vegitation, Crop Boundaries
        - Water Vs Land Segmentation, Flood Detection
        - Object Counting

    - Traffic / Street Analysis
        - Person Bounding Box
        - Car Bounding Box
        - Car Re-Identification
        - License Plate Identification
        - Person Re-Identification
    
    -  Autonomous Delivery
        - Drone flight path
        - Coco delivery

    - MRI Classification
        - 

    - Captcha To Gather Training Data
        - 

    - Chatbot
        - What are these components?
    
    - Product Review Analyzer
        - Sentiment Analysis
    
    - Copy Writer
        - Copy.ai
    
    - Pick the perfect NFL draft
    

* Human Performance Analysis
    - 4 key ML problems:
        - Classification (is there even a human in this image?)
        - Bounding box detection
        - Person Keypoints
        - Action classification based on keypoints
    - Conclusion
        - Link to 4 Oxen datasets



* Going from image to video in Human Performance Analysis
    - Naively running image models
    - Having some sort of history to take into account video
        - Performance issues, model export issues
    - Conclusion
        - Link to an Oxen dataset to get started

* Combining Pose Estimation Datasets
    - MSCoco 17 keypoints
    - AI challenger 14 keypoints
        - https://arxiv.org/abs/1711.06475
    - Crowdpose dataset
    - Leeds Sports dataset
        - http://sam.johnson.io/research/lsp.html
    - Need to combine to lowest common denominator
        - Leeds Sports 150x150 pixel and 14 keypoints
        - This can lead to performance hits
        - Might want to extend MSCoco to keep high quality images
    - Conclusion
        - Link to all these datasets

* Pose Dataset for your use case
    - Multi-human vs Single Pose
    - Pretrained on imagenet, some other subset?
    - Conclusion
        - Link to filtered down unsupervised or supervised datasets
        - Filter down dataset based on an image classifier
        - Call to improve/contribute/fork dataset for your use case

* Pose estimation evaluation technique (PCKh vs OKS)
    - Example code, based on Oxen format
    - oxen clone http://hub.oxen.ai/oxen/PoseEstimation
    - Conclusion
        - You should be constantly evaluating your models and understanding the metric

* Bounding box evaluation technique (IoU)
    - Example code, based on Oxen data format
    - oxen clone http://hub.oxen.ai/oxen/PersonBoundingBox
    - oxen checkout data branch

* Dataset licenses and what they mean?
    - Do research here

* Experiment: Image super resolution
    - Leeds sports is only 150x150
    - Can we upsample to 224x224?

* Experiment: Human image generation
    - Can we use stable diffusion + pose to generate new humans in same pose?


* Computer Vision Tasks (In terms of tech needed)
    - Image Classification
        - Hot Dog or Not?
        - Search/Filtering
        - Add Relevance
        - Topic Modeling/Clustering
        - Policy Enforcement
    - Object Detection
        - Bounding Box Around Object Type
            - Person, Animal, License Plate, Receipt, Product
        - Find all the people in this image
        - Crop subimage you are interested in
        - Damage and Defect Detection
        - Preduct Identification
        - Satelite Imagery
    - Object Keypoints
        - Human Pose Estimation
        - Face Keypoints
        - Animal joint keypoints
        - Hand Keypoints, Gestures
    - Object Segmentation
        - Pixel Level Segmentation of Objects
        - Crop background
    - Optical Character Recognition
        - License Plate Reading
        - Receipt Scanning
        - Product Identification
    - Prediction and Planning
        - Predict where the object you detected is going next
        - Predict where you should go next
        - Autonomous Vehicles
        - Delivery Robots
        - Delivery Drones