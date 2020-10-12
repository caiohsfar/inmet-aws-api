

const client = require('axios').default;
const datetime = require('node-datetime').create();

datetime.offsetInDays(-1);
const pastDay = datetime.format('Y-m-d');



const getStationsByType = async (type) => {
  const stationsUrl = `https://apitempo.inmet.gov.br/estacoes/${type}`;
  let peStations = [];
  try {
    const { data } = await client.get(stationsUrl);
    peStations = data.filter(item => item.SG_ESTADO == "PE");
  } catch (err) {
    console.log(err.message);
  }

  return peStations;
}


const getAllStations = async () => {
  const manuals = await getStationsByType("T");
  const auto = await getStationsByType("M");

  return [...manuals, ...auto];
}


const getDailyInfo = async () => {
  const stations = await getAllStations();
  
  const promisses = stations.map( async ({ CD_ESTACAO }) => {
    const dailyUrl = `https://apitempo.inmet.gov.br/estacao/diaria/${pastDay}/${pastDay}/${CD_ESTACAO}`
    let infos = [];
    try {
      const { data } = await client.get(dailyUrl);
      infos = data;
    } catch (err) {
      console.log(err.message)
    }
    return infos;
  })

  let formatedResults = []
  const response = await Promise.all(promisses);

  response.forEach(arrayOfAStation => {
    formatedResults = [...formatedResults, ...arrayOfAStation]
  })

  return formatedResults;
}



const successfullResponse = {
  isBase64Encoded: false,
  statusCode: 200,
  headers: { 'Content-Type': 'application/json' },
  body: []
};




exports.handler = async (event, context, callback) => {
    const allInfos  = await getDailyInfo();
    successfullResponse.body = allInfos;
    callback(null, successfullResponse);
};