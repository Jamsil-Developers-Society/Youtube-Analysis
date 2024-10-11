import axios from "axios";

// const apiURL = process.env.REACT_APP_APIURL;
const apiURL = "http://localhost:8000/api";

export function selectVideoList() {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/videos",

      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}

export function selectVideoDetail(video_id) {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/video",

      headers: {
        "Content-Type": "application/json",
      },
      params: { id: video_id },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}

export function selectVideoSessionList(video_id) {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/sessions",

      headers: {
        "Content-Type": "application/json",
      },
      params: { id: video_id },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}

export function selectCategories() {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/categories",

      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}

export function selectTotalData() {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/total",

      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}

export function selectGenrePeriodData() {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/genreperiod",

      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}

export function selectGenreRateData() {
  return new Promise((resolve, reject) => {
    axios({
      method: "GET",
      url: apiURL + "/genrerate",

      headers: {
        "Content-Type": "application/json",
      },
    })
      .then((res) => {
        resolve(res);
      })
      .catch((error) => {
        reject(error);
      });
  });
}
