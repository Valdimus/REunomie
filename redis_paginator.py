
# Copyright (C) 2019 NOUCHET Christophe
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <http://www.gnu.org/licenses/>.

# Author: Christophe Nouchet
# Email: nouchet.christophe@gmail.com
# Date: 03/04/2019


from redis import Redis

from typing import List


class RedisPaginatorException(BaseException):
    pass


class Element:

    def __init__(self, parameters, value):
        """
        :param parameters: All parameters of the element
        :param value: the value of the element
        """
        self.parameters = parameters
        self.value = value

    def __repr__(self):
        return "Element(parameters=%s, value=%s)" % (self.parameters, self.value)


class Page:

    def __init__(
            self, name: str, values: List[Element]=None, nb_page: int=0, nb_by_page: int= 0, total_nb_page: int=20, total: int=0,
            sort_by=None
    ):
        """
        :param name: The name of the Element
        :param values: All the element
        :param nb_page: The number of page
        :param total_nb_page: The total number of page
        :param total: Total element in the page
        """
        self.name = name
        self.values = values if values is not None else []
        self.nb_page = nb_page
        self.nb_by_page = nb_by_page
        self.total_nb_page = total_nb_page
        self.total = total
        self.sort_by = sort_by

    def add(self, element: Element):
        self.values.append(element)

    def __repr__(self):
        return "Page(name=%s, values=%s, nb_page=%s, total_nb_page=%s, total=%s, sort_by=%s)" % (
            self.name, self.values, self.nb_page, self.total_nb_page, self.total, self.sort_by
        )


class RedisPaginator:

    def __init__(self, redis: Redis, name: str, parameters: List[str]):
        self.redis = redis
        self.name = name
        self.parameters = parameters

    def __compute_name(self, parameters: dict):
        """The compute name. Don't call this function if you don't known what you do
        :param parameters: Parameters must be completed!
        :return:
        """
        return "%s_%s" % (
            self.name,
            '|'.join([
                parameters[i] for i in self.parameters
            ])
        )

    def _compute_name(self, parameters: dict):
        """Compute the name of the keys
        :param parameters: Parameters must be completed!
        :return:
        """

        if (
            len(parameters) != len(self.parameters) or
            sum([int(i in parameters) for i in self.parameters]) != len(self.parameters)
        ):
            raise RedisPaginatorException("Parameters are not valid! Get %s except %s" % (
                parameters.keys(), self.parameters
            ))

        return self.__compute_name(parameters)

    def _search_name(self, parameters):
        """The search name
        :param parameters: Parameters can be uncompleted
        :return:
        """
        return self.__compute_name({
            name: "*%s*" % parameters[name] if name in parameters else "*"
            for name in self.parameters
        })

    def add(self, parameters: dict, value: str):
        """Add an element
        :param parameters:
        :param value:
        :return:
        """
        self.redis.append(self._compute_name(parameters), value)

    def search(self, parameters: dict=None):
        """Search keys
        :param parameters: Parameters can be uncompleted
        :return:
        """
        return self.redis.keys(pattern=self._search_name(parameters if parameters is not None else {}))

    def rm(self, name: str):
        """Remove a key

        :param name:
        :return:
        """
        self.redis.delete(name)

    def get(self, name):
        """Get the value

        :param name:
        :return:
        """
        return str(self.redis.get(name))

    def extract_element(self, raw_element: str):
        """

        :param raw_element:
        :return:
        """
        temp = raw_element.replace("%s_" % self.name, "").split("|")

        return Element(
            parameters={
                    i: temp[self.parameters.index(i)]
                    for i in self.parameters
                }, value=self.get(raw_element)
        )

    def smart_search(self, parameters: dict=None, page: int=1, nb_by_page: int=20, sort_by: str=None):
        """Smart search with pagintator en sort function

        :param parameters: Parameters of the search
        :param page: The page to get
        :param nb_by_page:
        :param sort_by:
        :return:
        """

        # Check page
        no_page, real_page = page <= 0, page - 1

        found = self.search(parameters)

        sort_index = -1 if sort_by is None else self.parameters.index(sort_by) if sort_by in self.parameters else -1

        def filter(x):
            """Filter data"""
            if sort_index < 0:
                return str(x)
            temp = str(x).replace("%s_" % self.name, "").split("|")

            return temp[sort_index] if sort_index < len(temp) else str(x)

        found = sorted(found, key=lambda x: filter(x))
        size = len(found)
        nb_total_page = int(size / nb_by_page)
        # Pagination if necessary
        if no_page is False and 0 <= real_page <= nb_total_page:
            start_index, end_index = real_page * nb_by_page, (real_page + 1) * nb_by_page
            end_index = end_index if end_index < size else size - 1
            if 0 <= start_index < size and end_index > 0:
                found = found[start_index:end_index]
        else:
            page = 0

        page = Page(
            name=self.name, values=[], nb_page=page, nb_by_page=nb_by_page, total_nb_page=nb_total_page, total=size,
            sort_by=sort_by if sort_by is not None else self.parameters[0]
        )

        for element in found:
            value = str(element).replace("%s_" % self.name, "").split("|")

            page.add(Element(parameters={
                    i: value[self.parameters.index(i)]
                    for i in self.parameters
                }, value=self.get(element)
            ))

        return page
