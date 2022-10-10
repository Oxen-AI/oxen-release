# Open Source Models Are Halfway There

## The value of retraining deep learning models.

One of the reasons I love the AI community is their openness to share research and build on top of each other's work. There has always been a great tradition of publishing state of the art progress to Arxiv and publishing code to github.

Deep learning is unlocking the ability to analyze inputs that traditionally computers have had a difficult time understanding such as images, video, audio, and text. These inputs are natural to us as humans, yet can be quite the challenge when it comes to having the machine understand.

Frameworks like tensorflow, keras, and pytorch have made it relatively easy to build model graphs, and train deep neural networks that can solve these sensory input problems. With only a couple hundred lines of code, you can now train and export model graphs and publish inference pipelines that other people can use. 

Publishing inference graphs is great for getting started on a project without the overhead of training your own model from scratch. Training a deep learning model can take days, weeks, even months depending on the amount of data and the problem you are tackling.

Building an MVP with a model like this can be a great proof point that your end to end product will work. It is always good to test latency, accuracy, and other metrics that are important to product success before fully commiting to a model architecture, or even a using machine learning model at all.

Then comes the fateful day you start to test your ML product outside your own four walls. Deploying the model into the wild. You've had your computer vision model track your face around your office or apartment for days, you even tested your pose estimation model on every single one of your coworkers. The model was trained on hundreds of thousands of images, and should be robust to everything right? Wrong. Deep learning models are huge black boxes of millions of parameters, it is quite hard to know the long tail of data they will fail on, let alone how users will interact with them in their natural environment. 

Now you have to go back to the drawing board and see how you can improve the model. This depends on how you got it in the first place. If you simply grabbed the weights and inference pipeline from a research paper, you can still fine tune the model using some of the features from hidden layers, but this means they will be frozen and will no longer be fine tuned to your task.

The ideal situation is you have the data that the model was trained on, the hyper parameters it was trained with, as well as the model graph and weights themselves. 


# "Machine Inference" != "Machine Learning".

1) Model Inference vs Continuous Improvement
  - Frozen model graphs only get you so far
    - Will never get smarter
    - Might strip out some layers used in training for performance
  - We want to get to continuous improvement, put the learn back in machine learning. 
  - Data+full training pipeline is the missing piece that open sourcing inference graphs 

2) It is great that companies are open sourcing projects as proof points and integration points for MVPs
  - Need to test throughput, latency, baeline accuracy, memory usage, compute usage, etc

3) What happens when you deploy it into the real world?
  - It can and it will fail, are you prepared for what's next?
  - Give examples of common failures
    - Pushups, low light, background noise, blurry video, different skin tones, out of vocabulary, slang, misspellings, unknown phrases.

4) How do you fix it?
  - Can fine tune based off of features from a hidden layer
  - Need data from the distribution you want to fix
  - Would be optimal to know the distribution of data the frozen model graph was trained on
  - Would be best to have access to full model weights, full inference graph, full training pipeline, all hyper parameters used, AND your new data to truly solve the problem.

5) Where Oxen comes in
  - We are a version control system and collaboration hub optimized for deep learning data.
  - When you open source a model, you should open source the full model weights, hyper parameters used from training, training graph, as well as the original data it was trained on
  - This way while building on each other's shoulders we can go back to the source of the data, the root of the problem, and 
  - What if someone has an idea for a slight tweak on the architecture? They cannot try it unless they have the data.


# Other blog post ideas

- Deep learning overview, why open source has been amazing, but it's not quite there
- Sensory data, why get computers to think like us?
- Machine Learning != Machine Inference
- Fine tuning an open source inference model graph
- Start with zero shot training
  - Iterating until you have a model that works
- Tracking many experiments with lots of data
  - You have to be disciplined and patient
  - Can always iterate on data while model is training
    - give examples of problems in datasets
- Models+Data from scratch
  - Pose estimation
  - Fitness tracking
  - Animal identifier
  - Facial overlays
  - Receipt scanner
  - Understanding humans in dynamic video scenes (next level fitness tracking)
  - Sentiment analysis
  - Speaker identification

