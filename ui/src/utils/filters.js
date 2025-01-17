import Utils from "./utils";
import {getCurrentInstance} from "vue";
import {DATE_FORMAT_STORAGE_KEY} from "../components/settings/BasicSettings.vue";
import {TIMEZONE_STORAGE_KEY} from "../components/settings/BasicSettings.vue";
import moment from "moment-timezone";

export default {
    invisibleSpace: (value) => {
        return value.replaceAll(".", "\u200B" + ".");
    },
    humanizeDuration: (value, options) => {
        return Utils.humanDuration(value, options);
    },
    humanizeNumber: (value) => {
        return parseInt(value).toLocaleString(Utils.getLang());
    },
    cap: value => value ? value.toString().capitalize() : "",
    lower: value => value ? value.toString().toLowerCase() : "",
    date: (dateString, format) => {
        const currentLocale = getCurrentInstance().appContext.config.globalProperties.$moment().locale();
        const momentInstance = getCurrentInstance().appContext.config.globalProperties.$moment(dateString).locale(currentLocale);
        let f;
        if (format === "iso") {
            f = "YYYY-MM-DD HH:mm:ss.SSS";
        } else {
            f = format ?? localStorage.getItem(DATE_FORMAT_STORAGE_KEY) ?? "llll";
        }
        // Apply timezone and format using the correct locale
        return momentInstance
            .tz(localStorage.getItem(TIMEZONE_STORAGE_KEY) ?? moment.tz.guess())
            .format(f);
    }
}


